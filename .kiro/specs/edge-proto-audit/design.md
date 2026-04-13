# Design Document: Edge Proto Audit

## Overview

This design addresses the systematic gap between Tokeira's edge translation layer and the upstream Temporal API proto definitions (v1.43.0). The core problem: the translation pipeline silently drops proto fields, causing SDK examples to fail. The fix requires a field-by-field audit of every proto message Tokeira handles, data threading changes in kernel/runtime structs, missing command translations, history serializer fixes, and a long-poll mechanism for `GetWorkflowExecutionHistory`.

The guiding principle is **no silent drops**: every upstream proto field is either faithfully translated or explicitly documented as unsupported with an error.

## Architecture

The translation pipeline has three layers, each of which must be audited:

```
┌─────────────────────────────────────────────────────────────────┐
│                        SDK (upstream)                           │
└──────────────────────────────┬──────────────────────────────────┘
                               │ proto messages
┌──────────────────────────────▼──────────────────────────────────┐
│  grpc/translate.rs  — proto ↔ edge DTO translation              │
│  (request_to_edge, response_to_proto, command translation)      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ edge DTOs
┌──────────────────────────────▼──────────────────────────────────┐
│  translate/mod.rs — edge DTO definitions                        │
│  translate/to_internal.rs — edge DTO → kernel requests          │
│  translate/from_internal.rs — runtime structs → edge DTOs       │
└──────────────────────────────┬──────────────────────────────────┘
                               │ kernel/runtime types
┌──────────────────────────────▼──────────────────────────────────┐
│  tokeira-kernel (command.rs, event.rs)                           │
│  tokeira-runtime (runtime.rs — StartedActivityTask, etc.)       │
└─────────────────────────────────────────────────────────────────┘
```

For history (outbound only):
```
kernel HistoryEvent → history_serializer.rs → proto History → SDK
```

### Audit Methodology

For each proto message Tokeira handles:

1. List every field in the upstream `.proto` definition
2. Trace the field through the translation pipeline (proto → edge DTO → kernel/runtime)
3. Classify as: ✅ mapped, ❌ dropped, ⚠️ partially mapped, 🔲 not applicable
4. For dropped fields: determine if the kernel/runtime has the data, or if struct changes are needed

## Components and Interfaces

### Component 1: Command Translator Fixes (`grpc/translate.rs`)

**Current state:** `proto_command_to_workflow_command` handles 6 of 15 command types. The catch-all returns a generic error.

**Gap inventory for `ScheduleActivityTaskCommandAttributes`:**

| Proto field | Current mapping | Fix |
|---|---|---|
| `activity_id` | ✅ mapped | — |
| `activity_type` | ❌ dropped | Extract `.name` → new `activity_type` field on `WorkflowCommand::ScheduleActivity` |
| `task_queue` | ✅ mapped | — |
| `header` | ❌ dropped | Extract → new `header` field |
| `input` | ✅ mapped | — |
| `schedule_to_close_timeout` | ❌ dropped (set to `None`) | Extract duration → existing field |
| `schedule_to_start_timeout` | ❌ dropped (set to `None`) | Extract duration → existing field |
| `start_to_close_timeout` | ❌ dropped (set to `None`) | Extract duration → existing field |
| `heartbeat_timeout` | ❌ dropped (set to `None`) | Extract duration → existing field |
| `retry_policy` | ❌ dropped (set to `None`) | Extract → existing field |
| `request_eager_execution` | 🔲 not applicable (Tokeira doesn't support eager dispatch) | Document as unsupported |
| `use_workflow_build_id` | 🔲 not applicable | Document as unsupported |

**Missing command translations (proto → WorkflowCommand):**

| Proto command | WorkflowCommand variant | Status |
|---|---|---|
| `CancelTimerCommandAttributes` | `CancelTimer` | Exists in kernel, missing in translator |
| `RequestCancelActivityTaskCommandAttributes` | `RequestCancelActivity` | Exists in kernel, missing in translator |
| `ContinueAsNewWorkflowExecutionCommandAttributes` | `ContinueAsNew` | Exists in kernel, missing in translator |
| `StartChildWorkflowExecutionCommandAttributes` | `StartChildWorkflow` | Exists in kernel, missing in translator |
| `SignalExternalWorkflowExecutionCommandAttributes` | `SignalExternalWorkflowExecution` | Exists in kernel, missing in translator |
| `RequestCancelExternalWorkflowExecutionCommandAttributes` | `RequestCancelExternalWorkflowExecution` | Exists in kernel, missing in translator |
| `CancelWorkflowExecutionCommandAttributes` | `CancelWorkflow` | Exists in kernel, missing in translator |
| `RecordMarkerCommandAttributes` | `RecordMarker` | Exists in kernel, missing in translator |
| `ProtocolMessageCommandAttributes` | `ProtocolMessage` | Exists in kernel, missing in translator |
| `ScheduleNexusOperationCommandAttributes` | `ScheduleNexusOperation` | Exists in kernel, missing in translator |
| `RequestCancelNexusOperationCommandAttributes` | `CancelNexusOperation` | Exists in kernel, missing in translator |

**Design:** Add a match arm for each missing command type in `proto_command_to_workflow_command`. Each arm extracts all proto fields and maps them to the corresponding `WorkflowCommand` variant. The reverse direction (`workflow_command_to_proto`) already has stubs that return errors — these must be implemented too.

### Component 2: History Serializer Fixes (`history_serializer.rs`)

**Current state:** The serializer handles all `HistoryEventKind` variants but uses `_` patterns to ignore fields that should be mapped to proto attributes.

**Gap inventory (fields ignored with `_` patterns):**

| Event | Ignored field | Proto field it should map to |
|---|---|---|
| `ActivityTaskScheduled` | (missing `activity_type`) | `activity_type` on `ActivityTaskScheduledEventAttributes` |
| `ActivityTaskCompleted` | `activity_id` | `scheduled_event_id` (needs event ID tracking) |
| `ActivityTaskFailed` | `activity_id` | `scheduled_event_id` |
| `ActivityTaskTimedOut` | `activity_id`, `timeout_type` | `scheduled_event_id`, `retry_state` |
| `ActivityTaskCanceled` | `activity_id` | `scheduled_event_id`, `started_event_id` |
| `ActivityTaskCancelRequested` | `activity_id` | `scheduled_event_id` |
| `TimerStarted` | `fire_at` | `start_to_fire_timeout` (compute as `fire_at - event.happened_at`) |
| `WorkflowExecutionFailed` | `details`, `attempt` | `failure.details`, (attempt not in proto) |
| `WorkflowExecutionContinuedAsNew` | `workflow_execution_timeout` | `workflow_execution_timeout` is intentionally omitted in proto |
| `NexusOperationScheduled` | `operation_id` | No direct proto field (stored as `request_id` internally) |
| `NexusOperationStarted` | `operation_id` | `operation_id` field on proto |
| `NexusOperationCompleted` | `operation_id` | No direct proto field |
| `NexusOperationFailed` | `operation_id` | No direct proto field |
| `NexusOperationCanceled` | `operation_id` | No direct proto field |
| `NexusOperationTimedOut` | `operation_id` | No direct proto field |
| `WorkflowExecutionUpdateAccepted` | `update_name`, `input` | `accepted_request` (needs `update.v1.Request` construction) |
| `WorkflowExecutionUpdateCompleted` | `update_id`, `result` | `meta`, `outcome` |
| `WorkflowExecutionOptionsUpdated` | all fields | `versioning_override` |
| `StartChildWorkflowExecutionFailed` | `cause` | `cause` enum field |

**Design:** Fix each `_` pattern to map the kernel field to the corresponding proto field. For `TimerStarted.fire_at`, compute `start_to_fire_timeout` as `fire_at - happened_at`. For activity events that need `scheduled_event_id`, this requires the kernel to track the event ID linkage (or the serializer to look it up from context — simpler to add the field to the kernel event).

### Component 3: Data Threading Changes

All fields that the SDK expects in a poll response must be threaded from the originating command through kernel → runtime → edge. This is a single authoritative path — every field listed in the response gap inventories (Component 5) must have a corresponding field in the intermediate structs.

**Kernel changes (`WorkflowCommand::ScheduleActivity`):**

Add `activity_type: String` and `header: Option<Headers>` fields. The kernel already has timeout and retry_policy fields.

**Kernel changes (`HistoryEventKind::ActivityTaskScheduled`):**

Add `activity_type: String`, `header: Option<Headers>`, and `retry_policy: Option<RetryPolicy>` fields. The kernel records this event when processing the `ScheduleActivity` command, so it threads all fields from the command into the event.

**Runtime changes (`StartedActivityTask`):**

Add all fields needed to populate the full `PollActivityTaskQueueResponse`:
- `activity_type: String` — from `ActivityTaskScheduled` event
- `workflow_id: String` — from run metadata
- `workflow_type: String` — from run metadata
- `workflow_namespace: String` — from run metadata
- `header: Option<Headers>` — from `ActivityTaskScheduled` event
- `retry_policy: Option<RetryPolicy>` — from `ActivityTaskScheduled` event

The runtime's `poll_activity_task` creates `StartedActivityTask` from the kernel's activity state and run metadata — it must carry every field the edge layer needs.

**Edge DTO changes (`from_internal.rs`):**

`poll_activity_response` currently sets `activity_type: String::new()` and `workflow_id: String::new()`. After the runtime changes, ALL fields will be populated from `StartedActivityTask`.

### Component 4: Long-Poll for GetWorkflowExecutionHistory

**Current state:** The gRPC handler calls `read_history` from event 0 every time and returns immediately, even when `wait_new_event=true`. The edge DTO also drops `next_page_token`, so the server has no concept of "what the caller already saw."

**Design:**

The long-poll mechanism requires two fixes:

1. **Thread the caller's position**: Add `next_page_token: Vec<u8>` to the edge DTO. The token encodes the `last_event_id` the caller has already seen. When `wait_new_event=true`, the server uses this to determine what counts as "new." If the token is empty, the caller has seen nothing.

2. **Block until new events**: When `wait_new_event=true` and the current history has no events newer than the caller's position (or no events matching the filter), subscribe to a per-run watch channel and wait.

```rust
// In the edge service for GetWorkflowExecutionHistory:
let caller_last_event_id = decode_page_token(&req.next_page_token);

loop {
    let history = self.repo.read_history(run_key, 0, limit).await?;
    let current_last_event_id = history.last().map(|e| e.event_id).unwrap_or(0);

    // Apply filter
    let filtered = apply_event_filter(&history, filter_type);

    if !filtered.is_empty() || !req.wait_new_event {
        // Has matching events or caller doesn't want to wait
        return Ok(response_with_history(filtered, current_last_event_id));
    }

    if current_last_event_id > caller_last_event_id {
        // History advanced but no matching events — return empty with updated token
        return Ok(response_with_empty(current_last_event_id));
    }

    // No new events — wait for notification or timeout
    match tokio::time::timeout(Duration::from_secs(60), wait_handle.changed()).await {
        Ok(_) => continue,  // Re-read history
        Err(_) => return Ok(response_with_empty(current_last_event_id)),  // Timeout
    }
}
```

**Edge DTO changes:**

```rust
pub struct GetWorkflowExecutionHistoryRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub maximum_page_size: usize,
    pub wait_new_event: bool,
    pub history_event_filter_type: i32,
    pub next_page_token: Vec<u8>,    // NEW — caller's position cursor
}

pub struct GetWorkflowExecutionHistoryResponse {
    pub history: Vec<HistoryEvent>,
    pub next_page_token: Vec<u8>,    // NEW — encodes last_event_id for next call
}
```

**Page token encoding:** Simple `last_event_id` as big-endian i64 bytes. Empty token means "start from beginning."

**Implementation approach:** Use a `tokio::sync::watch` channel per run. The runtime already commits transitions that append history events — after each commit, send a notification on the watch channel. The long-poll handler subscribes to the channel and waits with `tokio::time::timeout`.

```
┌──────────────┐     commit_transition()     ┌──────────────┐
│   Runtime     │ ──────────────────────────► │ watch::Sender │
│   (lane.rs)   │                             │  per RunKey   │
└──────────────┘                              └───────┬───────┘
                                                      │ .changed()
                                              ┌───────▼───────┐
                                              │  Long-poll     │
                                              │  handler       │
                                              └───────────────┘
```

**Watch channel lifecycle:**
- Created lazily when the first long-poll request arrives for a run
- Stored in a `DashMap<RunKey, watch::Sender<i64>>` on the runtime (or edge service)
- The sender value is the latest `last_event_id`
- Cleaned up when the run closes (or via TTL)

### Component 5: Response Field Population

**`PollWorkflowTaskQueueResponse` gaps:**

| Proto field | Current | Fix |
|---|---|---|
| `previous_started_event_id` | ❌ missing | Thread from `StartedWorkflowTask` |
| `scheduled_time` | ❌ missing | Thread from WFT scheduled event timestamp |
| `started_time` | ❌ missing | Thread from WFT started event timestamp |
| `messages` | ❌ missing | Needed for update protocol; thread pending update messages |

**`PollActivityTaskQueueResponse` gaps:**

| Proto field | Current | Fix |
|---|---|---|
| `workflow_namespace` | ❌ missing | Thread namespace from run metadata |
| `workflow_type` | ❌ missing | Thread from run metadata |
| `workflow_execution` | ✅ mapped (after fix) | Needs `workflow_id` populated |
| `activity_type` | ❌ empty string | Thread from `ScheduleActivity` command |
| `header` | ❌ missing | Thread from command |
| `heartbeat_details` | ❌ missing | Thread from runtime heartbeat state |
| `scheduled_time` | ❌ missing | Thread from activity scheduled event |
| `current_attempt_scheduled_time` | ❌ missing | Thread from retry state |
| `started_time` | ❌ missing | Set to now at poll time |
| `retry_policy` | ❌ missing | Thread from command |

### Component 6: Full Request/Response Audit

The activity/history path is the most urgent, but Requirement 1 demands fidelity across ALL proto messages Tokeira handles. The following response builders also have known gaps:

**`workflow_execution_info_from_summary` (list responses):**

| Proto field | Current | Fix |
|---|---|---|
| `history_length` | ❌ hardcoded `0` | Thread from `WorkflowExecutionSummary` (needs storage query) |
| `state_transition_count` | ❌ hardcoded `0` | Thread from `WorkflowExecutionSummary` |
| `memo` | ❌ `Default::default()` | Thread from run metadata |
| `search_attributes` | ❌ `Default::default()` | Thread from run metadata |
| `execution_time` | ❌ `None` | Thread from run metadata |
| `parent_execution` | ❌ missing | Thread if child workflow |

**`start_request_to_edge` (StartWorkflowExecution):**

Fields like `workflow_execution_timeout`, `workflow_run_timeout`, `workflow_task_timeout`, `retry_policy`, `cron_schedule`, `header` are present in the upstream proto but may not be extracted.

**`describe_response_to_proto` (DescribeWorkflowExecution):**

The upstream `DescribeWorkflowExecutionResponse` has `pending_activities`, `pending_children`, `pending_workflow_task` fields that are not populated.

**Audit approach:** Each request/response translation function gets a field-by-field comparison against the upstream proto definition. Gaps are classified as:
- **Fix now**: data is available in the system, just not threaded
- **Fix later**: requires new storage queries or runtime features (document as known gap)
- **Not applicable**: Tokeira intentionally doesn't support the feature (document in UNSUPPORTED_FIELDS.md)

### Component 7: ActivityTaskStarted Event

**Problem:** The SDK's activity state machine requires the event sequence `ActivityTaskScheduled → ActivityTaskStarted → ActivityTaskCompleted`. The kernel currently skips `ActivityTaskStarted`, going directly from `Scheduled` to `Completed`. Without the `Started` event, the SDK cannot replay activity completions — the state machine cannot transition from `ScheduledEventRecorded` to `Started`.

This is the root cause of the hello-world example failure: the worker picks up the second workflow task (containing the activity completion in history) but can't replay it because the `ActivityTaskStarted` event is missing.

**Required event sequence (from upstream Temporal):**

```
event_id=N   ActivityTaskScheduled { activity_id, activity_type, task_queue, input, ... }
event_id=N+1 ActivityTaskStarted   { scheduled_event_id=N, identity, attempt }
event_id=N+2 ActivityTaskCompleted { scheduled_event_id=N, started_event_id=N+1, result }
```

**Kernel changes:**

1. Add `HistoryEventKind::ActivityTaskStarted { activity_id, scheduled_event_id, attempt, identity }` event variant
2. Add `apply_activity_started(activity_id, identity, now)` kernel operation that:
   - Looks up the activity in `state.activities`
   - Emits `ActivityTaskStarted` with `scheduled_event_id` from `ActivityState.schedule_event_id`
   - Records the `started_event_id` back into `ActivityState` (new field: `started_event_id: Option<i64>`)
   - Sets `ActivityState.started_at = now`
3. Add `scheduled_event_id: i64` and `started_event_id: i64` to `ActivityTaskCompleted`, `ActivityTaskFailed`, `ActivityTaskTimedOut`, `ActivityTaskCanceled` event variants — populated from `ActivityState` when the resolution is applied

**Runtime changes:**

When `poll_activity_task` dispatches a task to a worker, call `kernel.apply_activity_started()` to emit the started event. This creates a new interaction pattern: the runtime calls back into the kernel during activity dispatch, not just during workflow task completion.

**History serializer changes:**

- Add serialization for `ActivityTaskStarted` → `ActivityTaskStartedEventAttributes`
- Populate `scheduled_event_id` and `started_event_id` on completion/failure/timeout/cancel events from the kernel event data

## Data Models

### Modified: `WorkflowCommand::ScheduleActivity`

```rust
ScheduleActivity {
    activity_id: String,
    activity_type: String,          // NEW
    task_queue: TaskQueueName,
    input: Payloads,
    header: Option<Headers>,        // NEW
    retry_policy: Option<RetryPolicy>,
    deployment: Option<DeploymentId>,
    build_id: Option<BuildId>,
    schedule_to_close_timeout: Option<Duration>,
    schedule_to_start_timeout: Option<Duration>,
    start_to_close_timeout: Option<Duration>,
    heartbeat_timeout: Option<Duration>,
}
```

### Modified: `HistoryEventKind::ActivityTaskScheduled`

```rust
ActivityTaskScheduled {
    activity_id: String,
    activity_type: String,          // NEW
    task_queue: TaskQueueName,
    input: Payloads,
    header: Option<Headers>,        // NEW
    retry_policy: Option<RetryPolicy>, // NEW
    schedule_to_close_timeout: Option<Duration>,
    schedule_to_start_timeout: Option<Duration>,
    start_to_close_timeout: Option<Duration>,
    heartbeat_timeout: Option<Duration>,
}
```

### Modified: `StartedActivityTask`

```rust
pub struct StartedActivityTask {
    pub run_key: RunKey,
    pub activity_id: String,
    pub activity_type: String,      // NEW
    pub workflow_id: String,        // NEW
    pub workflow_type: String,      // NEW
    pub workflow_namespace: String, // NEW
    pub task_queue: TaskQueueName,
    pub token: ActivityTaskToken,
    pub input: Payloads,
    pub header: Option<Headers>,    // NEW
    pub attempt: u32,
    pub retry_policy: Option<RetryPolicy>, // NEW
    pub schedule_to_close_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
}
```

### New: `HistoryWaitHandle`

```rust
/// Per-run notification channel for long-poll subscribers.
pub struct HistoryWaitHandle {
    sender: tokio::sync::watch::Sender<i64>,
    receiver: tokio::sync::watch::Receiver<i64>,
}
```

### Modified: Edge DTO `PollActivityTaskQueueResponse`

```rust
pub struct PollActivityTaskQueueResponse {
    pub task_token: Vec<u8>,
    pub activity_id: String,
    pub activity_type: String,
    pub workflow_id: String,
    pub workflow_type: String,       // NEW
    pub workflow_namespace: String,  // NEW
    pub run_key: RunKey,
    pub input: Payloads,
    pub header: Option<Headers>,     // NEW
    pub attempt: u32,
    pub retry_policy: Option<RetryPolicy>, // NEW
    pub schedule_to_close_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
}
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Command translation round-trip

*For any* supported proto `Command` with all attribute fields populated, translating to `WorkflowCommand` via `proto_command_to_workflow_command` and back to proto via `workflow_command_to_proto` SHALL preserve all field values.

**Validates: Requirements 1.3**

### Property 2: History serialization field completeness

*For any* kernel `HistoryEvent` with all fields of its `HistoryEventKind` variant populated (non-default), serializing via `history_event_to_proto` SHALL produce a proto `HistoryEvent` where every attribute field that has a corresponding kernel field is populated (not at proto default).

**Validates: Requirements 1.4**

### Property 3: Timestamp and Duration conversion round-trip

*For any* `OffsetDateTime` value (within reasonable range) and *for any* `time::Duration` value (non-negative, < 100 years), converting to proto `Timestamp`/`Duration` via `to_proto_timestamp`/`to_proto_duration` and converting back SHALL produce the original value (within nanosecond precision).

**Validates: Requirements 1.5**

### Property 4: Activity data threading end-to-end

*For any* `ScheduleActivityTaskCommandAttributes` proto message with `activity_type`, `activity_id`, all four timeouts, `retry_policy`, and `input` populated, the resulting `PollActivityTaskQueueResponse` proto (after flowing through command translation → kernel event → runtime `StartedActivityTask` → edge DTO → proto response) SHALL contain the same `activity_type.name`, `activity_id`, timeout values, and `input`.

**Validates: Requirements 2.1, 2.3**

## Error Handling

### Unsupported command types
When `proto_command_to_workflow_command` encounters a command type that Tokeira does not implement, it returns `ProtoConversionError::UnsupportedCommand(command_type_name)` — a new error variant that clearly identifies the unsupported command rather than the current generic "no proto Command equivalent" message.

### Unsupported proto fields
Fields that Tokeira intentionally does not support (e.g., `request_eager_execution`, `use_workflow_build_id`, cron schedules) are documented in a `UNSUPPORTED_FIELDS.md` file in the edge crate. The translation code includes comments referencing this document.

### Long-poll timeout
The long-poll handler uses `tokio::time::timeout(Duration::from_secs(60), ...)`. On timeout, it returns the current history (which may have no close events). This matches Temporal server behavior.

### Watch channel cleanup
If a run closes, the watch channel sender is dropped. Any pending long-poll receivers will see the channel closed and should re-read history one final time before returning.

## Testing Strategy

### Property-based tests (using `proptest`)

Each correctness property is implemented as a `proptest` test with minimum 100 iterations:

1. **Command round-trip** — Generate random proto commands for each supported type, translate to `WorkflowCommand` and back, assert field equality. Tag: `Feature: edge-proto-audit, Property 1: Command translation round-trip`

2. **History field completeness** — Expand the existing `arb_history_event_kind` generator to populate ALL fields (currently some are hardcoded to `None`/default). Assert that serialized proto attributes have non-default values for every field the kernel provides. Tag: `Feature: edge-proto-audit, Property 2: History serialization field completeness`

3. **Timestamp/Duration round-trip** — Generate random `OffsetDateTime` and `time::Duration` values, convert to proto and back, assert equality. Tag: `Feature: edge-proto-audit, Property 3: Timestamp/Duration conversion round-trip`

4. **Activity data threading** — Generate random `ScheduleActivityTaskCommandAttributes`, flow through the full pipeline (mocking the runtime/storage layer), assert the `PollActivityTaskQueueResponse` contains matching values. Tag: `Feature: edge-proto-audit, Property 4: Activity data threading end-to-end`

### Unit tests (example-based)

- Each newly implemented command translation (CancelTimer, RequestCancelActivity, ContinueAsNew, StartChildWorkflow, etc.) gets a golden-example test with known input/output
- Each history serializer fix gets a golden-example test verifying the previously-dropped field is now populated
- Long-poll handler: test immediate return when `wait_new_event=false`, test blocking behavior with mock watch channel

### Integration tests (SDK examples)

Run each SDK example against tokeirad as end-to-end validation:
- `hello_world` — validates basic activity scheduling, data threading, and workflow completion
- `activity_heartbeating` — validates heartbeat recording
- `timer_examples` — validates timer start/fire/cancel
- `message_passing` — validates signals, queries, updates
- `child_workflows` — validates child workflow lifecycle
- `continue_as_new` — validates continue-as-new command translation
- `cancellation` — validates cancel command translation and cleanup

These are run manually or in CI, not as property tests (they require a running server and are expensive).
