# Design Document: Edge Poll Response Fidelity

## Overview

This design closes four SDK-correctness gaps in the poll response and start response translation pipeline. The most critical is `previous_started_event_id` — without it, every workflow task replays from the beginning of history because the SDK cannot determine the sticky replay boundary. The remaining three gaps (`started` on start response, `WorkflowTaskScheduled` attributes, and poll response timestamps) are lower severity but still affect SDK behavior.

All four data-threading fixes follow the same pattern: enrich a kernel or runtime struct with data that already exists in the system, then thread it through to the edge layer's proto translation. The kernel remains pure and deterministic. Component 7 (WFT timeout enforcement) adds a runtime background scanner — the only component that introduces new I/O.

## Architecture

The data flow for each fix follows the standard translation pipeline:

```
┌─────────────────────────────────────────────────────────────────┐
│  Kernel (pure state machine)                                     │
│  WorkflowState, PendingWorkflowTask, HistoryEventKind            │
│  ─ adds previous_started_event_id to state                       │
│  ─ enriches WorkflowTaskScheduled with task_queue/timeout/attempt│
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│  Runtime (orchestration)                                         │
│  StartedWorkflowTask, StartWorkflowResult                        │
│  ─ threads previous_started_event_id, timestamps to edge         │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│  Edge Layer (proto translation)                                  │
│  DTOs, from_internal.rs, grpc/translate.rs                       │
│  ─ populates proto fields from enriched DTOs                     │
└─────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### Component 1: Kernel — `previous_started_event_id` tracking

**Problem:** The SDK uses `previous_started_event_id` (proto field 4 on `PollWorkflowTaskQueueResponse`) to determine where to start replay. Without it, every WFT replays from event 1. The kernel already tracks `PendingWorkflowTask.started_event_id` for the current WFT, but does not remember the `started_event_id` of the *previous* completed WFT.

**Design:**

Add a `previous_started_event_id: i64` field to `WorkflowState`. This field is 0 when no WFT has been completed, and is set to the `started_event_id` of the most recently completed WFT.

The update happens in `apply_workflow_task_completed`: before clearing `pending_workflow_task`, copy `pending.started_event_id.unwrap()` into `state.previous_started_event_id`.

The replay path (`replay_history_prefix`) must also reconstruct this field by tracking the `started_event_id` of each `WorkflowTaskCompleted` event it encounters.

**Files changed:**
- `crates/tokeira-kernel/src/state.rs` — add `previous_started_event_id: i64` to `WorkflowState`
- `crates/tokeira-kernel/src/kernel.rs` — set the field in `apply_workflow_task_completed` and `replay_history_prefix`

### Component 2: Kernel — `WorkflowTaskScheduled` event enrichment

**Problem:** The `WorkflowTaskScheduled` event currently carries only `logical_seq`. The SDK expects `task_queue`, `start_to_close_timeout`, and `attempt` in the proto attributes.

**Design:**

Enrich `HistoryEventKind::WorkflowTaskScheduled` with three new fields:

```rust
WorkflowTaskScheduled {
    logical_seq: LogicalTaskSeq,
    task_queue: TaskQueueName,           // NEW — from state.task_queue
    workflow_task_timeout: Duration,     // NEW — from state.workflow_task_timeout
    attempt: u32,                        // NEW — always 1 for fresh schedule, incremented on retry
}
```

The `TransitionBuilder::schedule_workflow_task` method already has access to `self.state.task_queue` and `self.state.workflow_task_timeout`. The attempt value comes from the kernel's WFT attempt tracking on `WorkflowState`:

- **Fresh schedule** (after completion, signal, activity resolution, etc.): attempt is 1.
- **Retry schedule** (after `apply_workflow_task_failed` or `apply_workflow_task_timed_out`): the kernel increments the attempt counter before re-scheduling. The `WorkflowTaskScheduled` event carries the incremented attempt.

This matches the Temporal server, where `executionInfo.WorkflowTaskAttempt` is set to 1 on fresh schedules and incremented in `failWorkflowTask` before the retry schedule. The `WorkflowTaskScheduled` event reads the current attempt from execution info.

To support this, `WorkflowState` needs a `workflow_task_attempt: u32` field (default 1) that is:
- Reset to 1 in `apply_workflow_task_completed` (successful completion resets the counter)
- Incremented in `apply_workflow_task_failed` and `apply_workflow_task_timed_out` before re-scheduling
- Read by `schedule_workflow_task` to populate the event's `attempt` field

The `replay_history_prefix` method must also handle the new fields when replaying `WorkflowTaskScheduled` events.

**Files changed:**
- `crates/tokeira-kernel/src/event.rs` — add fields to `WorkflowTaskScheduled` variant
- `crates/tokeira-kernel/src/kernel.rs` — populate fields in `schedule_workflow_task`, handle in `apply_replayed_event`

### Component 3: History Serializer — `WorkflowTaskScheduled` attributes

**Problem:** The history serializer emits `WorkflowTaskScheduledEventAttributes` with `..Default::default()`, dropping `task_queue`, `start_to_close_timeout`, and `attempt`.

**Design:**

After Component 2 enriches the kernel event, the serializer destructures the new fields and populates the proto:

```rust
HistoryEventKind::WorkflowTaskScheduled {
    logical_seq: _,
    task_queue,
    workflow_task_timeout,
    attempt,
} => Attributes::WorkflowTaskScheduledEventAttributes(
    history::WorkflowTaskScheduledEventAttributes {
        task_queue: Some(task_queue_from_domain(task_queue)),
        start_to_close_timeout: Some(to_proto_duration(*workflow_task_timeout)),
        attempt: *attempt as i32,
        ..Default::default()
    },
),
```

**Files changed:**
- `crates/tokeira-edge/src/translate/history_serializer.rs` — populate fields in `WorkflowTaskScheduled` arm

### Component 4: Runtime — `StartedWorkflowTask` enrichment

**Problem:** `StartedWorkflowTask` currently carries only `run_key`, `workflow_id`, `task_queue`, and `token`. The edge layer needs `previous_started_event_id`, `scheduled_time`, and `started_time` to populate the poll response.

**Design:**

Add three fields to `StartedWorkflowTask`:

```rust
pub struct StartedWorkflowTask {
    pub run_key: RunKey,
    pub workflow_id: WorkflowId,
    pub task_queue: TaskQueueName,
    pub token: WorkflowTaskToken,
    pub previous_started_event_id: i64,      // NEW
    pub scheduled_time: OffsetDateTime,       // NEW
    pub started_time: OffsetDateTime,         // NEW
}
```

In `start_workflow_task_inner`, after the kernel applies `WorkflowTaskStarted`:
- `previous_started_event_id` comes from `new_state.previous_started_event_id`
- `scheduled_time` comes from the `WorkflowTaskScheduled` event's `happened_at` — the runtime can find this by looking up the event at `pending.scheduled_event_id` in the committed history, or more efficiently, by reading it from the kernel state. Since the kernel's `PendingWorkflowTask` doesn't currently carry the scheduled timestamp, the simplest approach is to read it from the last committed history.

However, reading history for a single timestamp is wasteful. A better approach: add `scheduled_at: OffsetDateTime` to `PendingWorkflowTask` in the kernel, set it in `schedule_workflow_task` from `self.now`. Then the runtime reads it from `new_state.pending_workflow_task.scheduled_at`.

For `started_time`, the runtime already has `now` (the wall-clock time when `apply_workflow_task_started` was called). This is the same `now` passed to the kernel command. The runtime can use this directly.

**Files changed:**
- `crates/tokeira-kernel/src/state.rs` — add `scheduled_at: OffsetDateTime` to `PendingWorkflowTask`
- `crates/tokeira-kernel/src/kernel.rs` — set `scheduled_at` in `schedule_workflow_task`
- `crates/tokeira-runtime/src/runtime.rs` — add fields to `StartedWorkflowTask`, populate in `start_workflow_task_inner`

### Component 5: Edge Layer — poll response proto population

**Problem:** `poll_response_to_proto` uses `..Default::default()` for `previous_started_event_id`, `scheduled_time`, and `started_time`.

**Design:**

The edge DTO `PollWorkflowTaskQueueResponse` gains three fields:

```rust
pub struct PollWorkflowTaskQueueResponse {
    pub task_token: Vec<u8>,
    pub started_event_id: i64,
    pub attempt: u32,
    pub payload: WorkflowTaskPayloadDto,
    pub queries: HashMap<String, WorkflowQueryDto>,
    pub messages: Vec<ProtocolMessageDto>,
    pub previous_started_event_id: i64,          // NEW
    pub scheduled_time: Option<OffsetDateTime>,   // NEW
    pub started_time: Option<OffsetDateTime>,     // NEW
}
```

`from_internal::poll_response` populates these from `StartedWorkflowTask`.

`grpc/translate.rs::poll_response_to_proto` maps them to proto fields:

```rust
workflowservice::PollWorkflowTaskQueueResponse {
    // ... existing fields ...
    previous_started_event_id: resp.previous_started_event_id,
    scheduled_time: resp.scheduled_time.map(|t| to_proto_timestamp(t)),
    started_time: resp.started_time.map(|t| to_proto_timestamp(t)),
    ..Default::default()
}
```

**Files changed:**
- `crates/tokeira-edge/src/translate/mod.rs` — add fields to `PollWorkflowTaskQueueResponse`
- `crates/tokeira-edge/src/translate/from_internal.rs` — populate from `StartedWorkflowTask`
- `crates/tokeira-edge/src/grpc/translate.rs` — map to proto fields

### Component 6: Edge Layer — `started` field on `StartWorkflowExecutionResponse`

**Problem:** `start_response_to_proto` uses `..Default::default()` for the `started` field (proto field 3). The edge layer already distinguishes `Started` vs `UsedExisting` in `workflow_service.rs::start_workflow_execution`, but the DTO doesn't carry the distinction.

**Design:**

Add `started: bool` to `StartWorkflowExecutionResponse` DTO:

```rust
pub struct StartWorkflowExecutionResponse {
    pub run_key: RunKey,
    pub run_id: RunId,
    pub transition_seq: u64,
    pub last_event_id: i64,
    pub started: bool,              // NEW
}
```

In `workflow_service.rs::start_workflow_execution`:
- `StartWorkflowResult::Started` → set `started: true`
- `StartWorkflowResult::UsedExisting` → currently returns an error, but if the conflict policy allows it, set `started: false`

In `grpc/translate.rs::start_response_to_proto`:
```rust
workflowservice::StartWorkflowExecutionResponse {
    run_id: resp.run_id.0.to_string(),
    started: resp.started,
    ..Default::default()
}
```

Note: The current `start_workflow_execution` returns `EdError::WorkflowAlreadyStarted` for `UsedExisting`, so `started` is always `true` in the success path today. The field is still needed for future conflict policy support (e.g., `USE_EXISTING` returning the existing run_id with `started=false`).

**Files changed:**
- `crates/tokeira-edge/src/translate/mod.rs` — add `started: bool` to `StartWorkflowExecutionResponse`
- `crates/tokeira-edge/src/translate/from_internal.rs` — set `started: true` in `start_response`
- `crates/tokeira-edge/src/grpc/translate.rs` — map `started` to proto field
- `crates/tokeira-edge/src/workflow_service.rs` — set `started` based on `StartWorkflowResult` variant

### Component 7: Runtime — WFT start-to-close timeout enforcement

**Problem:** The kernel has a `WorkflowTaskTimedOut` command and the corresponding `apply_workflow_task_timed_out` handler, but the runtime has no mechanism to detect when a started WFT has exceeded its `start_to_close_timeout`. Without this, unresponsive workers block workflow progress indefinitely.

**Design:**

The runtime needs a background scanner that periodically checks started WFTs for timeout violations. The scanner uses the `started_at` timestamp on `PendingWorkflowTask` (added in Component 4) and the `workflow_task_timeout` on `WorkflowState` to compute the deadline.

The scanner follows the same non-authoritative pattern as the timer scanner and activity timeout scanner: it detects the condition and submits a `Command::WorkflowTaskTimedOut` to the kernel via the lane. The kernel is the authority — if the WFT has already been completed or the run is closed, the kernel rejects the command harmlessly.

**Timestamp safety:** All timeout arithmetic uses `time::OffsetDateTime` (UTC) and `time::Duration`. DST boundaries are not a concern because:
- `OffsetDateTime::now_utc()` returns an absolute UTC instant — no local timezone is involved
- `time::Duration` is a fixed span, not calendar-aware — adding a duration to a UTC instant is monotonic
- The proto timestamp conversion (`to_proto_timestamp`) uses `unix_timestamp()` which is UTC-based

The one subtlety is wall-clock vs monotonic time. `OffsetDateTime::now_utc()` reads the system wall clock, which can jump backward on NTP corrections or VM suspend/resume. For timeout enforcement this means a backward jump could delay timeout detection (harmless — the scanner retries on the next cycle) or a forward jump could trigger premature timeouts (unlikely in practice, and the kernel's rejection of stale commands provides a safety net). If this becomes a real concern, the scanner could use `tokio::time::Instant` for elapsed-time tracking alongside the authoritative `OffsetDateTime` for history recording.

**Note on `jiff`:** The `jiff` crate excels at calendar-aware datetime arithmetic (DST-safe date math, timezone handling). Tokeira's timeout use case is purely duration-based arithmetic on UTC instants, where `time::OffsetDateTime` is sufficient. `jiff` would become valuable when implementing schedule/cron support (Feature 6 in the umbrella spec), where "fire at 9am local time every day" requires DST-aware calendar math.

**Implementation approach:**

The WFT timeout scanner follows the same runtime-local tracking pattern as the existing `WorkflowTimeoutTrackingState`. A new `WftTimeoutTrackingState` (in-memory `HashMap<RunKey, WftTimeoutEntry>` behind `Arc<Mutex>`) tracks started WFTs:

```rust
pub struct WftTimeoutEntry {
    pub run_key: RunKey,
    pub shard_id: ShardId,
    pub logical_seq: LogicalTaskSeq,
    pub started_event_id: i64,
    pub started_at: OffsetDateTime,
    pub workflow_task_timeout: Duration,
}
```

The tracking state is populated when the runtime starts a WFT (`start_polled_workflow_task`) and removed when the WFT completes, fails, or times out. On each scan cycle:

1. Snapshot the tracking state for the active shard
2. For each entry, compute `deadline = started_at + workflow_task_timeout`
3. If `now > deadline`, submit `Command::WorkflowTaskTimedOut` to the run's lane
4. On success or kernel rejection, remove the entry from tracking

This avoids any new storage query surface for the hot path — the scanner operates entirely on runtime-local state. However, recovery after restart or shard failover requires repopulating the tracking state from durable storage, following the same pattern as `WorkflowTimeoutTrackingState` and `ActivityTrackingState`.

**Recovery:** On shard acquisition, `sweep_shard` calls a new `list_runs_with_started_wfts_for_shard(shard_id, limit)` storage query that returns runs with a started (but not completed) WFT. Each result is inserted into `WftTimeoutTrackingState`. This mirrors the existing `list_runs_with_workflow_timeouts_for_shard` → `WorkflowTimeoutTrackingState` pattern.

**Files changed:**
- `crates/tokeira-kernel/src/state.rs` — add `started_at: Option<OffsetDateTime>` to `PendingWorkflowTask`
- `crates/tokeira-kernel/src/kernel.rs` — set `started_at` in `apply_workflow_task_started`
- `crates/tokeira-runtime/src/wft_timeout.rs` (new module) — `WftTimeoutTrackingState`, `WftTimeoutEntry`, `evaluate_wft_timeout`, `scan_wft_timeouts_once`, `run_wft_timeout_scanner`
- `crates/tokeira-runtime/src/runtime.rs` — integrate tracking state into `start_polled_workflow_task` and `complete_workflow_task`
- `crates/tokeira-storage/src/api.rs` — add `list_runs_with_started_wfts_for_shard` query and `WftSweepEntry` return type
- `crates/tokeira-storage/src/memory.rs` — implement the query for `InMemoryStore`
- `crates/tokeira-runtime/src/recovery.rs` — add `WftTimeoutTrackingState` parameter to `sweep_shard`, repopulate from storage

## Data Models

### Modified: `WorkflowState` (kernel)

```rust
pub struct WorkflowState {
    // ... existing fields ...
    /// started_event_id of the most recently completed WFT.
    /// 0 when no WFT has been completed yet.
    pub previous_started_event_id: i64,     // NEW
}
```

### Modified: `PendingWorkflowTask` (kernel)

```rust
pub struct PendingWorkflowTask {
    pub logical_seq: LogicalTaskSeq,
    pub scheduled_event_id: i64,
    pub started_event_id: Option<i64>,
    pub attempt: u32,
    pub scheduled_at: OffsetDateTime,       // NEW
    pub started_at: Option<OffsetDateTime>, // NEW — set when WorkflowTaskStarted is applied
}
```

### Modified: `HistoryEventKind::WorkflowTaskScheduled` (kernel)

```rust
WorkflowTaskScheduled {
    logical_seq: LogicalTaskSeq,
    task_queue: TaskQueueName,              // NEW
    workflow_task_timeout: Duration,        // NEW
    attempt: u32,                           // NEW
}
```

### Modified: `StartedWorkflowTask` (runtime)

```rust
pub struct StartedWorkflowTask {
    pub run_key: RunKey,
    pub workflow_id: WorkflowId,
    pub task_queue: TaskQueueName,
    pub token: WorkflowTaskToken,
    pub previous_started_event_id: i64,     // NEW
    pub scheduled_time: OffsetDateTime,     // NEW
    pub started_time: OffsetDateTime,       // NEW
}
```

### Modified: `PollWorkflowTaskQueueResponse` (edge DTO)

```rust
pub struct PollWorkflowTaskQueueResponse {
    pub task_token: Vec<u8>,
    pub started_event_id: i64,
    pub attempt: u32,
    pub payload: WorkflowTaskPayloadDto,
    pub queries: HashMap<String, WorkflowQueryDto>,
    pub messages: Vec<ProtocolMessageDto>,
    pub previous_started_event_id: i64,         // NEW
    pub scheduled_time: Option<OffsetDateTime>,  // NEW
    pub started_time: Option<OffsetDateTime>,    // NEW
}
```

### Modified: `StartWorkflowExecutionResponse` (edge DTO)

```rust
pub struct StartWorkflowExecutionResponse {
    pub run_key: RunKey,
    pub run_id: RunId,
    pub transition_seq: u64,
    pub last_event_id: i64,
    pub started: bool,                          // NEW
}
```

## Correctness Properties

### Property 1: previous_started_event_id invariant

*For any* sequence of kernel transitions on a single run that includes N completed workflow tasks (N ≥ 1), the `WorkflowState.previous_started_event_id` after the Nth completion SHALL equal the `started_event_id` of the Nth `WorkflowTaskCompleted` event. When N = 0, `previous_started_event_id` SHALL be 0.

**Validates:** Requirement 1, Acceptance Criteria 1.1, 1.2, 1.3, 1.6

### Property 2: WorkflowTaskScheduled serialization completeness

*For any* `HistoryEvent` with kind `WorkflowTaskScheduled` where `task_queue` is non-empty, `workflow_task_timeout` is positive, and `attempt` is ≥ 1, serializing via `history_event_to_proto` SHALL produce a `WorkflowTaskScheduledEventAttributes` where `task_queue` has a non-empty `name`, `start_to_close_timeout` has positive `seconds` or `nanos`, and `attempt` is ≥ 1.

**Validates:** Requirement 3, Acceptance Criteria 3.2, 3.3, 3.4, 3.5

### Property 3: Poll response proto projection

*For any* `PollWorkflowTaskQueueResponse` edge DTO with `previous_started_event_id` set to a non-negative value and `scheduled_time`/`started_time` set to valid timestamps, `poll_response_to_proto` SHALL produce a proto response where `previous_started_event_id` equals the DTO value, `scheduled_time` is a valid `Timestamp`, and `started_time` is a valid `Timestamp`.

**Validates:** Requirement 1 (AC 1.5), Requirement 4 (AC 4.2, 4.3, 4.4)

### Property 4: Start response started field

*For any* `StartWorkflowExecutionResponse` edge DTO with `started = true`, `start_response_to_proto` SHALL produce a proto response with `started = true`. *For any* DTO with `started = false`, the proto response SHALL have `started = false`.

**Validates:** Requirement 2, Acceptance Criteria 2.1, 2.2, 2.3

### Property 5: WFT timeout enforcement correctness

*For any* run with a started WFT where `started_at + workflow_task_timeout < now`, the runtime's timeout scanner SHALL produce a `Command::WorkflowTaskTimedOut`. *For any* run where `started_at + workflow_task_timeout ≥ now`, no timeout command SHALL be produced. *For any* run where the WFT has already been completed before the scanner fires, the kernel SHALL reject the timeout command harmlessly.

**Validates:** Requirement 3 (AC 3.6, 3.7), Requirement 4 (AC 4.5, 4.6)

## Error Handling

No new error paths are introduced for the data-threading changes. All changes add data to existing success paths:
- `previous_started_event_id` defaults to 0 (correct for first WFT)
- `scheduled_time` and `started_time` are `Option<OffsetDateTime>` — `None` maps to proto default (absent timestamp)
- `started` defaults to `true` in the current success path (new workflows always return `started = true`)
- `WorkflowTaskScheduled` fields come from kernel state that is always populated when a WFT is scheduled

For the WFT timeout scanner:
- The scanner is non-authoritative. If the kernel rejects a `WorkflowTaskTimedOut` command (WFT already completed, run closed, seq mismatch), the rejection is a harmless no-op — the scanner logs and moves on.
- If the scanner fails to query storage, it retries on the next scan cycle. No WFTs are lost — they remain in authoritative state until the scanner successfully detects them.

## Testing Strategy

### Property-based tests (proptest, 100 iterations)

1. **previous_started_event_id kernel invariant** — Generate random sequences of Start → (Signal → )* WFT-Schedule → WFT-Start → WFT-Complete cycles. After each completion, assert `state.previous_started_event_id == completed_wft.started_event_id`. After the first schedule (before any completion), assert `state.previous_started_event_id == 0`.

2. **WorkflowTaskScheduled serialization** — Generate arbitrary `HistoryEvent` values with `WorkflowTaskScheduled` kind (varying task_queue names, timeout durations, attempt counts). Serialize to proto and assert all three fields are non-default.

3. **Poll response proto projection** — Extend the existing `property_poll_response_projection` test to assert `previous_started_event_id`, `scheduled_time`, and `started_time` are correctly mapped.

4. **Start response started field** — Generate arbitrary `StartWorkflowExecutionResponse` DTOs with random `started` values. Assert `start_response_to_proto` preserves the value.

### Unit tests (example-based)

- Kernel: first WFT has `previous_started_event_id = 0`, second WFT has it set to the first WFT's `started_event_id`
- Kernel: `WorkflowTaskScheduled` event carries task_queue, timeout, attempt after enrichment
- History serializer: `WorkflowTaskScheduledEventAttributes` proto has populated fields
- Edge: `poll_response_to_proto` populates `previous_started_event_id`, `scheduled_time`, `started_time`
- Edge: `start_response_to_proto` populates `started = true`

### Integration tests

- Run `hello_world` SDK example — validates the full pipeline including `previous_started_event_id` (the second WFT should replay only from the boundary, not from event 1)
