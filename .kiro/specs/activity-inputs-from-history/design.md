# Activity Inputs From History — Bugfix Design

## Overview

An activity's input and header stay in its ActivityTaskScheduled event, and nowhere else:
- **No copies.** The run's state, the activity's dispatch row and its backlog entries no longer carry them. The state keeps each copy's position in its stored layout, written empty, so states that the previous release wrote still decode.
- **One read at the start.** Starting an activity task reads the scheduled event with one lookup, which returns the history batch that holds it. The task carries that event's input and header.
- **A new state version.** This release writes the run's state under a new envelope version, which the previous release refuses, so a downgrade fails loudly instead of delivering empty inputs.

## Glossary

- **Copies:** the input and header in an activity's entry of the run's state, the input in its dispatch row (`activity_dispatch.input_data`), and the input in a backlog entry for its task (`BacklogPayload::Activity`).
- **Scheduled event:** the activity's ActivityTaskScheduled history event. Its id is the activity's `schedule_event_id`.
- **Start:** the transition that marks an activity task started and answers the worker that polled for it, or that receives it eagerly (`start_activity_task_inner`).
- **Retired field:** a field position kept in a stored layout after its data has moved. It is written empty, and anything an earlier release wrote in it is read and dropped.
- **Previous release:** the release before this change, which writes the copies.

## How this maps onto Tokeira's architecture

v1.31.0 keeps the input and header only in history, and its start reads the scheduled event. Tokeira does the same, with four departures.

1. **No batch id.** v1.31.0 stores, beside the scheduled event's id, the id of the batch that holds the event, and reads the event through that batch (`ScheduledEventBatchId`; `executions.proto:525`, `mutable_state_impl.go:1462-1474 @ v1.31.0`). A Tokeira run's history batches are keyed by their first event id, and they cover the run's event ids from 1 without gaps or overlaps. That is true of a reset successor's copied batches too ([bounded-bulk-writes](../bounded-bulk-writes/design.md)). So the batch that holds an event is the one with the greatest first event id not after the event's id, and one lookup by the event's id finds it, with no batch id to store.
2. **No events cache.** v1.31.0 caches events, the scheduled event among them, when it writes them (`service/history/events/cache.go @ v1.31.0`), so most starts don't touch persistence. Here every start reads the store once.
3. **An eager start reads too.** v1.31.0 answers an eager start from the command it has just recorded (`workflow_task_completed_handler.go:594-598 @ v1.31.0`). Here an eager start takes the same path as a polled one and reads the event that the workflow task completion has just committed. The values are the same, since the event is written from the command.
4. **Retired fields.** v1.31.0's protobuf state drops a field and keeps decoding. Tokeira's state is positional postcard, so the copies' positions stay, written empty. A new envelope version marks the change for the previous release.

## Bug Details

### Bug Condition

The bug condition holds when a run has pending activities with inputs or headers. Every commit of such a run writes the copies again: the inputs and headers in its state, and the inputs in the activities' dispatch rows and backlog entries. On DSQL the condition fails a run once its pending inputs, together, take its state over 1,048,576 bytes (1.1). It fails a spill page once a backlog entry's copy takes the entry over that limit (1.4).

### Examples

- **Many pending inputs.** A workflow schedules one activity per workflow task, each with an input of about 200 KB. Every history batch is about 200 KB, but the state holds every pending input. With six activities pending, the state passes 1 MiB, DSQL refuses it, and the workflow task that scheduled the sixth fails, as does every later commit of the run. After the fix the state holds none of the inputs and stays small.
- **A heartbeat.** An activity heartbeats once a second while ten others wait, each with a 50 KB input. Every heartbeat rewrites 550 KB of input with the state. After the fix it rewrites only the state.
- **A retry.** A failed activity is retried. Its retry writes its input into its dispatch row again. After the fix the row carries no input.

## Expected Behavior

### Preservation Requirements

- A delivered task's other fields stay as they are (3.1).
- A start's refusals and their order stay the same (3.2).
- History stays as it is (3.3).
- Restoring an activity's original options still reads the scheduled event by its id (3.4).
- Nexus operations and standalone activities are untouched (3.5).
- The run growth limits refuse the same runs at the same sizes (3.6).

## Root Cause

- **Scheduling.** The kernel copies the command's input and header into the activity's entry in the run's state (`apply_workflow_command`, `ScheduleActivity`). It also copies the input into the `DispatchOp::EnqueueActivityTask` it emits.
- **Replay.** A replay copies them again from the scheduled event (`apply_replayed_event`, `ActivityTaskScheduled`).
- **Re-dispatch.** Re-dispatch reads the input back from the state (`enqueue_activity_dispatch`), and so does the runtime's retry (`activity_dispatch_task`).
- **Storage.** Storage writes the dispatch op's input into the dispatch row and into the in-memory store's entry. The grace scanner copies the broker task's input into the backlog entry.
- **Start.** The start answers from the state's copy, so the copies were never needed for delivery.

## Correctness Properties

Property 1: No copies of an activity's input or header

_For any_ run, any activities scheduled with any inputs and headers, and any sequence of starts, heartbeats, retries, option updates, pauses, unpauses, resets of activities, re-dispatches and spills, the following SHALL hold after each step:
- The run's encoded state SHALL equal the encoding of the same state whose activities were scheduled with empty inputs and no headers.
- No dispatch row and no backlog entry SHALL hold input bytes.

**Validates: Requirements 2.1**

Property 2: A delivered task carries its scheduled event's input and header

_For any_ activity and any delivery path, the delivered task's input and header SHALL equal those of the activity's ActivityTaskScheduled event in the run's history. The paths are:
- a poll;
- an eager start;
- a delivery after a retry, an options update, a pause and unpause, or a reset of the activity;
- a task re-published by recovery, a backlog drain or dispatch reconciliation;
- a reset successor's activity that was scheduled in its copied history.

**Validates: Requirements 2.2, 2.5**

Property 3: One lookup finds the scheduled event

_For any_ history cut into batches of any sizes, and any event id:
- `read_history_event` SHALL return exactly the event with that id when the history holds it, and none when it doesn't.
- On DSQL it SHALL issue one statement, which returns at most one batch.

Batches of one event, of many events, and a reset successor's copied batches are all included.

**Validates: Requirements 2.3**

Property 4: A start whose read fails changes nothing

_For any_ start whose scheduled-event read fails or finds no event:
- the run's state and its dispatch rows SHALL be unchanged;
- the task SHALL be back in its queue;
- the start SHALL answer the read's error, or an internal error when the event is missing.

**Validates: Requirements 2.4**

Property 5: This release reads the previous release's data, and the previous release refuses this release's state

_For any_ state, backlog entry or dispatch row that the previous release wrote, with any inputs and headers, this release SHALL:
- read it;
- deliver the activity's scheduled event's input;
- write the run's next state with empty retired fields.

The previous release's state decoder SHALL refuse every state this release writes.

**Validates: Requirements 2.6, 2.7**

## Fix Implementation

### Retired fields (`crates/tokeira-kernel/src/state.rs`)

- `RetiredField<T>` is a zero-sized type for a field whose data has moved:
  - It serializes as `T::default()`.
  - It deserializes by reading a `T` and dropping it.
- It holds no data, so no code can put an input back in a state by mistake.
- `ActivityState`'s `input: Payloads` and `header: Option<Headers>` become `retired_input: RetiredField<Payloads>` and `retired_header: RetiredField<Option<Headers>>`, in the same positions.
- An empty `Payloads` and a `None` header each encode as one byte. So a new state's activity encodes as an activity of the previous release with an empty input and no header would have.
- A state the previous release wrote decodes with the same layout. Its inputs and headers are dropped as it is read (2.6).
- `ActivityState`'s doc comment stops claiming it carries everything a re-dispatch needs. The scheduled event carries the input and header.

### Kernel

- **Scheduling.** Scheduling an activity still writes the scheduled event with the command's input and header (3.3). The activity's entry in the state gets none.
- **Replay.** Replaying an ActivityTaskScheduled event copies neither.
- **Re-dispatch.** `DispatchOp::EnqueueActivityTask` loses its `input`, and `enqueue_activity_dispatch` stops reading it.
- **Other commands.** Nothing else in the kernel reads an activity's input or header.

### Storage: the scheduled-event read (`RunRepository::read_history_event`)

- The new method is `read_history_event(run_key, event_id) -> Result<Option<HistoryEvent>>`.
- **Default implementation.** It reads `read_history(run_key, event_id - 1, 1)` and keeps the event only if its id matches. That is correct on any store, and the test doubles use it.
- **DSQL.** One read statement: `SELECT first_event_id, last_event_id, events_data FROM history_batch WHERE run_key = $1 AND first_event_id <= $2 ORDER BY first_event_id DESC LIMIT 1`.
  - The primary key `(run_key, first_event_id)` serves it as one index seek, and it returns at most one row: the batch that holds the event, by departure 1.
  - It decodes that batch, and returns the event with the id, or none.
  - The general read is no substitute. `READ_HISTORY_BATCHES_SQL` filters on `last_event_id`, which no index covers. So it walks the run's batches from the first until one matches, and each statement fetches up to 64 batches (`HISTORY_BATCH_PAGE`).
- **Memory.** A search of the run's ordered history by event id.
- **Wrappers.** The `Arc` implementation and the edge's `HistoryNotifyingRepository` forward the method. Otherwise they would fall back to the default and to the general read.
- **Cost on the poll path.** The read adds one read per start: one row, as large as the batch that holds the event. On DSQL that is at most 1,048,576 bytes, the value limit. A reset successor's copied batches hold at most 512 KiB of events each.
- **Restoring options.** `original_activity_options` reads the scheduled event with this method too (3.4).

### Storage: dispatch rows, backlog entries, the codec and the snapshot

- **`DispatchableActivityTask`** loses `input`, and its `PartialEq` stops comparing it.
- **Dispatch rows.** The DSQL dispatch row keeps its `input_data` column, which is `NOT NULL`, and every insert and update writes the encoded empty payload list, one byte. Reads stop selecting the column, so a row the previous release wrote, input and all, reads the same. The in-memory store's entry holds the task without an input.
- **Backlog entries.** `BacklogPayload::Activity`'s `input` becomes `retired_input: RetiredField<Payloads>`, in its position.
  - New entries carry an empty input.
  - An entry the previous release wrote decodes as before, and its input is dropped.
  - The backlog envelope keeps its version. An entry stays readable by both releases, and the previous release can't load the state of a run this release has written anyway.
- **The state codec.**
  - `encode_workflow_state` writes a new envelope version.
  - `decode_workflow_state` accepts the new version and the previous one, `WORKFLOW_STATE_ENVELOPE_VERSION` before this change. Both decode with the same layout.
  - The previous release checks for its own version only, so it refuses a new state with `BlobFormatError` (2.7).
  - This release's `BlobFormatError` message names a newer release, as well as one before the envelopes, as a possible writer of a version it doesn't know.
- **The memory snapshot.** `SNAPSHOT_FORMAT_VERSION` becomes 5, since `DispatchableActivityTask` and `DispatchOp` change. As its policy says, a snapshot of an older version is refused, not migrated.

### Runtime: the start (`start_activity_task_inner`)

- **When the read runs.** The read comes after every check that can refuse the start, so a refused start reads nothing and keeps its order (3.2). The checks cover:
  - the attempt and the stamp;
  - an activity that is started, paused or gone;
  - a closed run;
  - workflow rules;
  - deployment transitions.
- **What it does.** It reads the scheduled event with `read_history_event(run_key, schedule_event_id)` before the started transition commits. The answer's input and header come from the event's `ActivityTaskScheduled` attributes; every other field comes from where it does today (3.1).
- **When it fails.** If the read fails, or finds no event, the start commits nothing. The task goes back to its queue, as when the start's commit fails, and the start answers the read's error, or an internal error naming the missing event (2.4).
- **Eager starts.** An eager start goes through this path too (departure 3).

### Runtime: re-publishing

The publisher, the backlog drain, dispatch reconciliation, recovery, an options update's re-publish, and a retry all build tasks without an input. The broker's task loses the field with `DispatchableActivityTask`.

### The run growth limits' measure

`measured_state_len` becomes the state's encoded size ([run-growth-limits](../run-growth-limits/bugfix.md) criterion 2.9). The inputs it subtracted are no longer in the state. The retired fields add two bytes per activity.

### Upgrade and downgrade

- **Upgrade.** Upgrade with every node stopped, as the release note already says; this change adds no second note. The new release then reads:
  - states of the previous version;
  - backlog entries and dispatch rows that carry inputs.
  
  Each run's first write drops its inputs. No pass converts stored data.
- **Downgrade.** Downgrade after the upgrade is unsupported. The previous release refuses the states this release writes, with `BlobFormatError`. It does not deliver activities with empty inputs.

### Specs this changes

The code PR aligns these specs with the fix:
- [runtime-activity-pump](../runtime-activity-pump/requirements.md): the scheduling criterion stops filling the activity's input in its state.
- [dsql-side-tables](../dsql-side-tables/requirements.md): `input_data` is written empty and not read.
- [run-growth-limits](../run-growth-limits/bugfix.md): criterion 2.9 and its design measure the state as its encoded size.
- [activity-state-writes](../activity-state-writes/bugfix.md) and [bounded-bulk-writes](../bounded-bulk-writes/bugfix.md): their Out of Scope points here.
- [history-pagination](../history-pagination/bugfix.md): criterion 2.8 reads the scheduled event with the one-batch lookup.

## Testing Strategy

### Exploratory Bug Condition Checking

These tests are written and run on the code before the fix, and each must fail there:
- **Property 1, in memory.** For generated inputs, the encoded state grows with each pending input.
- **Live DSQL, the first example.** Six pending activities with inputs of about 200 KB each make DSQL refuse the run's state. The workflow task that schedules the sixth fails.

After the fix, both pass.

### Live DSQL

The suite runs on an ephemeral cluster under the `dsql-live` profile ([docs/testing/dsql-live-suites.md](../../../docs/testing/dsql-live-suites.md)). It covers:
- **The first example.** The commits succeed, and each activity, once started, is delivered with its own input and header.
- **A multi-event batch.** One workflow task schedules three activities, and the second activity is delivered with its own input.
- **A reset successor.** An activity whose scheduled event lies in the middle of one of the successor's 512 KiB copied batches is delivered with its input.
- **The previous release's data.**
  - A dispatch row written with an input reads without it.
  - A backlog entry written with an input drains, and its activity is delivered from history.

### Property-Based Tests

- Properties 1, 2, 4 and 5 run in `tokeira-runtime` against the in-memory store, over generated inputs, headers and delivery paths.
- Property 3 runs in `tokeira-storage` on the in-memory store, over generated batch cuts. The live suite covers it on DSQL.

### Unit Tests

- **The read.** `read_history_event` on both stores covers:
  - one-event batches and a multi-event batch;
  - a reset successor's copied batches;
  - an id past the end of the history.
- **The start.** A refused start reads nothing. A failed read leaves the state and the dispatch rows unchanged, and returns the task to its queue.
- **The state codec.**
  - The previous release's frozen state bytes decode, and their inputs and headers are dropped.
  - A new state carries the new envelope version.
  - A decoder that checks for the previous version only refuses it.
- **Other stores.**
  - A backlog entry the previous release wrote decodes.
  - A dispatch row written with an input reads without it.
  - The snapshot version is 5.
- **The measure.** The measured state equals the encoded state.

### Negative controls

Each control is patched in alone, then run, then reversed so that the tree is byte-identical. Each must fail the test that covers it:
- the scheduled event's input copied into the state again;
- a dispatch row or a backlog entry written with the input;
- the start answering from the state, which then holds no input;
- the DSQL lookup without its descending order, which finds the first batch;
- the lookup with `<` for `<=`, which misses an event that opens its batch;
- the read moved before a refusing check;
- a failed read that drops the task instead of returning it to its queue;
- the decoder refusing the previous version;
- the encoder writing the previous version.

### Preservation Checking

- The existing activity, retry, timeout, pause, reset, recovery, backlog and run growth tests stay green.
- Some runtime tests seed an activity without a scheduled event in history. Those that start the activity now seed the event too.
