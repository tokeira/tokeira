# Edge History Serializer Completeness — Bugfix Design

## Overview

The edge history serializer (`history_serializer.rs`) translates kernel `HistoryEventKind` variants into Temporal proto `HistoryEvent` messages. Many proto attribute fields are defaulted to zero/empty where SDKs expect populated values, causing replay state-machine errors, missing metadata in UI/CLI tools, and incorrect SDK routing decisions.

The fix is stratified into four implementation classes. The most impactful pattern — `workflow_task_completed_event_id` — affects ~15 events and requires a threading strategy that respects the kernel-purity constraint. This design formalizes the bug condition, classifies every missing field, and specifies how context flows into the serializer without violating architectural boundaries.

## Glossary

- **Bug_Condition (C)**: A proto field that has authoritative non-empty data available but the serializer emits the field's default/zero value
- **Property (P)**: For every field where authoritative data exists, the serializer SHALL emit that data in the proto output
- **Preservation**: All currently-correct field mappings SHALL continue to produce identical proto values after the fix
- **`workflow_task_completed_event_id`**: The event ID of the `WorkflowTaskCompleted` event whose command batch produced a given event. SDKs use this to correlate commands with their producing WFT during replay.
- **Serializer-only fix**: The kernel event already carries the value; only `history_serializer.rs` needs a code change
- **Kernel event enrichment**: A new field must be added to `HistoryEventKind` before the serializer can wire it
- **Runtime/history-context enrichment**: The value is derived from relationships between events or runtime state; must be passed explicitly to the serializer
- **Deferred proto-sync**: The field depends on v1.62-specific proto surface not yet available in `tokeira_proto`

## Bug Details

### Bug Condition

The bug manifests when the serializer has access to authoritative non-empty data for a proto field (either directly on the kernel event, derivable from explicit context, or discarded via `_` bindings) and still emits the field's default/zero value. The bug is conditional: optional Temporal fields SHALL remain default when no authoritative value was authored for that event path.

**Formal Specification:**
```
FUNCTION isBugCondition(event, field)
  INPUT: event of type HistoryEvent, field of type ProtoFieldDescriptor
  OUTPUT: boolean

  LET authoritative_value = resolveAuthoritativeValue(event, field)
  LET serialized_value = serialize(event).getField(field)

  RETURN authoritative_value IS NOT NULL
         AND authoritative_value != DEFAULT_VALUE(field)
         AND serialized_value == DEFAULT_VALUE(field)
END FUNCTION
```

### Examples

- `NexusOperationCompleted { operation_id: "op-123", .. }` → proto `operation_id` is empty because the serializer binds `operation_id: _` (serializer-only fix)
- `WorkflowExecutionCompleted { result }` → proto `workflow_task_completed_event_id` is 0 because the kernel event doesn't carry the producing WFT event ID (kernel enrichment)
- `WorkflowExecutionStarted { request_id: "req-abc", .. }` → proto `identity` is set to `"req-abc"` instead of the originating client identity (kernel enrichment — the kernel doesn't carry client identity separately from request_id)
- `WorkflowExecutionPaused { .. }` → proto uses `WorkflowExecutionCanceledEventAttributes` which misleads SDK consumers (encoding fix)

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**
- All fields listed in requirements 3.1–3.11 continue to produce identical proto values
- `serialize_history()` continues to produce valid protobuf bytes decodable as `temporal.api.history.v1.History`
- Deprecated v0.4 wire-compat fields (`ContinuedAsNew.failure`, `SignalExternal.control`, `RequestCancelExternal.control`, `NexusStarted.operation_id`) continue to be populated
- The serializer remains a pure function: `&[HistoryEvent] → Vec<u8>` (or with explicit context parameter)

**Scope:**
All inputs where the bug condition does NOT hold — fields that are correctly wired today, fields that are intentionally defaulted because no authoritative value exists — must produce byte-identical proto output after the fix.

## Hypothesized Root Cause

The serializer was written incrementally as kernel event variants were added. Each branch focused on the fields the kernel carried at that time, using `..Default::default()` for everything else. Four distinct root causes:

1. **Discarded bindings**: The kernel carries the value but the serializer explicitly ignores it via `_` patterns (e.g., Nexus `operation_id` on terminal events, `WorkflowExecutionContinuedAsNew.workflow_execution_timeout`, `WorkflowExecutionContinuedAsNew.retry_policy`)

2. **Missing kernel fields**: The kernel event model was designed for internal correctness, not proto completeness. Fields like `workflow_task_completed_event_id`, worker identity on activity completions, retry state on activity failures, and namespace on child/external events were never added because the kernel doesn't need them for state-machine transitions.

3. **Missing context threading**: Some values (like `workflow_task_completed_event_id`) are properties of the *relationship* between events, not of a single event. The serializer's current signature `&HistoryEvent → Attributes` cannot express these without either enriching the event or passing explicit context.

4. **Placeholder encodings**: Pause/unpause events have no upstream proto type, so they were mapped to `WorkflowExecutionCanceledEventAttributes` as a temporary measure that was never revisited.

## Correctness Properties

Property 1: Bug Condition — Serializer emits authoritative data for all classified fields

_For any_ history event where a proto field has been classified as serializer-only, kernel-enrichment (after enrichment), or runtime-context (after context is provided), the serializer SHALL emit the authoritative value for that field rather than the field's default/zero value.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 2.13, 2.14, 2.15, 2.16, 2.17, 2.18, 2.19, 2.20, 2.21, 2.22, 2.23, 2.24, 2.25, 2.26, 2.27, 2.28, 2.29**

Property 2: Preservation — Existing correct serialization unchanged

_For any_ history event and proto field where the current serializer already produces the correct value, the fixed serializer SHALL produce a byte-identical proto output for that event, preserving all existing correct behavior.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 3.11**

## Fix Implementation

### Implementation Classification Table

#### Class 1: Serializer-Only Fixes

These require only changes to `history_serializer.rs`. The kernel already carries the data.

| Event | Field | Current Issue |
|-------|-------|---------------|
| `NexusOperationCompleted` | `operation_id` | Bound as `_`, not wired to proto |
| `NexusOperationFailed` | `operation_id` | Bound as `_`, not wired to proto |
| `NexusOperationCanceled` | `operation_id` | Bound as `_`, not wired to proto |
| `NexusOperationTimedOut` | `operation_id` | Bound as `_`, not wired to proto |
| `WorkflowExecutionContinuedAsNew` | `workflow_execution_timeout` | Bound as `_`, not wired to proto |
| `WorkflowExecutionContinuedAsNew` | `retry_policy` | Bound as `_`, not wired to proto |
| `SignalExternalWorkflowExecutionInitiated` | `target_run_id` | Already carried, already wired (confirm) |
| `RequestCancelExternalWorkflowExecutionInitiated` | `target_run_id` | Already carried, already wired (confirm) |

#### Class 2: Kernel Event Enrichment

New fields must be added to `HistoryEventKind` variants in `crates/tokeira-kernel/src/event.rs`.

| Event | Field(s) to Add | Source |
|-------|-----------------|--------|
| `WorkflowExecutionStarted` | `identity: String`, `header: Option<Headers>` | Start request metadata |
| `WorkflowExecutionCancelRequested` | `identity: String`, `external_initiated_event_id: i64` | Cancel request metadata |
| `WorkflowExecutionCanceled` | `details: Option<Payloads>` | Cancel command payload |
| `WorkflowExecutionTimedOut` | `new_execution_run_id: Option<RunId>` | Timeout retry path |
| `WorkflowExecutionSignaled` | `header: Option<Headers>` | Signal request headers |
| `WorkflowTaskStarted` | `request_id: String` | Task start metadata |
| `WorkflowTaskCompleted` | `sdk_metadata: Option<Payload>`, `worker_version: Option<String>` | Completion response |
| `WorkflowTaskFailed` | `worker_version: Option<String>` | Failure response |
| `ActivityTaskStarted` | `request_id: String`, `last_failure: Option<Payload>` | Activity start metadata |
| `ActivityTaskCompleted` | `identity: WorkerIdentity` | Worker that completed |
| `ActivityTaskFailed` | `identity: WorkerIdentity`, `retry_state: RetryState` | Worker + retry resolution |
| `ActivityTaskTimedOut` | `retry_state: RetryState` | Retry resolution |
| `ActivityTaskCanceled` | `identity: WorkerIdentity` | Worker that reported cancel |
| `StartChildWorkflowExecutionInitiated` | `namespace: String`, `header: Option<Headers>`, `memo: Memo`, `search_attributes: SearchAttributes`, `workflow_execution_timeout: Option<Duration>`, `workflow_run_timeout: Option<Duration>`, `workflow_task_timeout: Duration`, `retry_policy: Option<RetryPolicy>`, `cron_schedule: Option<String>` | Child-start command |
| `StartChildWorkflowExecutionFailed` | `initiated_event_id: i64`, `namespace: String`, `workflow_type: WorkflowType` | Failure context |
| `ChildWorkflowExecutionCompleted` | `namespace: String`, `child_run_id: RunId` | Child metadata |
| `ChildWorkflowExecutionFailed` | `namespace: String`, `child_run_id: RunId`, `retry_state: RetryState` | Child metadata |
| `ChildWorkflowExecutionCanceled` | `namespace: String`, `child_run_id: RunId`, `workflow_type: WorkflowType`, `details: Option<Payloads>` | Child metadata |
| `ChildWorkflowExecutionTerminated` | `namespace: String`, `workflow_type: WorkflowType` | Child metadata |
| `ChildWorkflowExecutionTimedOut` | `namespace: String`, `workflow_type: WorkflowType`, `retry_state: RetryState` | Child metadata |
| `ExternalWorkflowExecutionSignaled` | `namespace: String`, `target_run_id: Option<RunId>` | Signal result |
| `SignalExternalWorkflowExecutionFailed` | `namespace: String`, `target_run_id: Option<RunId>` | Signal failure |
| `ExternalWorkflowExecutionCancelRequested` | `namespace: String`, `target_run_id: Option<RunId>` | Cancel result |
| `RequestCancelExternalWorkflowExecutionFailed` | `namespace: String`, `target_run_id: Option<RunId>` | Cancel failure |
| `SignalExternalWorkflowExecutionInitiated` | `namespace: String`, `header: Option<Headers>` | Signal command |
| `RequestCancelExternalWorkflowExecutionInitiated` | `namespace: String` | Cancel command |
| `NexusOperationScheduled` | `nexus_header: Option<Headers>`, `endpoint_id: String` | Schedule command |
| `WorkflowExecutionUpdateAccepted` | `accepted_request_sequencing_event_id: i64` | Update tracking |
| `WorkflowExecutionUpdateCompleted` | `accepted_event_id: i64` | Update tracking |
| `WorkflowExecutionUpdateRejected` | `rejected_request_message_id: String`, `rejected_request_sequencing_event_id: i64` | Update tracking |

#### Class 3: Runtime/History-Context Enrichment — `workflow_task_completed_event_id`

This is the most impactful missing field, affecting ~15 events. The value represents "which WFT produced the command that created this event."

**Affected events:**
- `WorkflowExecutionCompleted`
- `WorkflowExecutionFailed`
- `WorkflowExecutionCanceled`
- `WorkflowExecutionContinuedAsNew`
- `ActivityTaskScheduled`
- `ActivityTaskCancelRequested`
- `ActivityTaskCanceled` (cancel command's WFT)
- `TimerStarted`
- `TimerCanceled`
- `MarkerRecorded`
- `StartChildWorkflowExecutionInitiated`
- `SignalExternalWorkflowExecutionInitiated`
- `RequestCancelExternalWorkflowExecutionInitiated`
- `NexusOperationScheduled`
- `WorkflowTaskCompleted` (self-referential — not needed)

**Design Decision: Stamp at commit time in the lane**

The `workflow_task_completed_event_id` is known at the moment the lane commits a WFT completion transition: the `WorkflowTaskCompleted` event is emitted first, then all command-produced events follow in the same transition batch. The lane knows the WFT completed event ID because it just assigned it.

**Chosen approach: Add `workflow_task_completed_event_id: i64` to each affected `HistoryEventKind` variant.**

Rationale:
- The kernel stays pure — the lane (in `tokeira-runtime`) stamps the field before committing the transition. The kernel's `apply_commands()` method receives the WFT completed event ID as a parameter and threads it into each produced event.
- The serializer remains a pure projector — no lookup, no state, just reads the field from the event.
- No history-context map needed at serialization time.
- Storage cost is one `i64` per affected event (negligible).

**Alternative considered and rejected:** A `SerializationContext` struct passed alongside events containing a `HashMap<i64, i64>` mapping event_id → producing_wft_event_id. Rejected because:
- Requires the caller to build the map before serialization
- Adds complexity to the serializer's API surface
- The data is static per event and belongs on the event itself

**Implementation sketch:**

```rust
// In tokeira-kernel, apply_commands gains a parameter:
pub fn apply_commands(
    &mut self,
    commands: Vec<Command>,
    wft_completed_event_id: i64,  // new
) -> Vec<HistoryEvent> { ... }

// Each command-produced event includes the field:
HistoryEventKind::ActivityTaskScheduled {
    workflow_task_completed_event_id: i64,  // stamped by apply_commands
    activity_id: String,
    // ...
}
```

**Other runtime-context fields:**

| Event | Field | Source | Threading |
|-------|-------|--------|-----------|
| `WorkflowTaskStarted` | `history_size_bytes` | Run's accumulated history size | Lane stamps before commit |
| `WorkflowTaskStarted` | `suggest_continue_as_new` | Derived from history_size_bytes threshold | Lane stamps before commit |
| `ActivityTaskCancelRequested` | `scheduled_event_id` | Resolve `activity_id` → scheduled event ID | Lane maintains activity registry |
| `TimerCanceled` | `started_event_id` | Resolve `timer_id` → started event ID | Lane maintains timer registry |

These follow the same pattern: the lane (runtime) has the context and stamps the event before commit. The kernel's `apply_commands` receives the necessary lookup closures or pre-resolved values.

#### Class 4: Deferred Proto-Sync

These fields depend on proto surface delivered by `temporal-api-v1.62-sync` (now complete) or on feature specs not yet implemented. Each is assigned to a specific spec for resolution.

| Event | Field | Owning Spec | Status |
|-------|-------|-------------|--------|
| `NexusOperationStarted` | `operation_token` (rename of `operation_id`) | `temporal-api-v1.62-sync` | Proto available — implement in this spec's Phase 6 |
| `WorkflowExecutionStarted` | `workflow_execution_expiration_time` | `temporal-compatibility` | Implement when compatibility spec lands |
| `WorkflowExecutionStarted` | `source_version_stamp` | `worker-deployments` | Implement when deployment versioning lands |
| `WorkflowExecutionStarted` | `completion_callbacks` | `temporal-compatibility` | Implement when compatibility spec lands |
| `WorkflowTaskCompleted` | `metering_metadata` | `observability-production` | Implement when production observability lands |
| `WorkflowExecutionOptionsUpdated` | `versioning_override` (proto mapping) | `worker-deployments` | Implement when deployment versioning lands |
| `ActivityTaskScheduled` | `use_workflow_build_id` | `worker-deployments` | Implement when deployment versioning lands |
| `WorkflowExecutionStarted` | `first_workflow_task_backoff` | `temporal-compatibility` | Implement when compatibility spec lands |

#### Intentionally Defaulted Fields (Documented Rationale)

Fields that remain default because no authoritative source exists today. Each is assigned to the spec that will unblock it.

| Event | Field | Rationale | Unblocked By |
|-------|-------|-----------|--------------|
| `WorkflowExecutionStarted` | `initiator` | Only meaningful for retried/cron starts; requires kernel enrichment to distinguish start context. | This spec (Phase 3, task 5.1) — reclassified as kernel enrichment |
| `WorkflowExecutionContinuedAsNew` | `header` | Only populated if the CAN command carries headers. Command model enrichment needed. | This spec (Phase 3, task 5.7) — add header to CAN command |
| `WorkflowExecutionContinuedAsNew` | `inherit_build_id` | Versioning feature not yet implemented. | `worker-deployments` |
| `ChildWorkflowExecutionStarted` | `header` | Only populated if the child-started path carries an authored header. | This spec (Phase 3, task 5.4) — add header to child-started event |
| `WorkflowTaskCompleted` | `binary_checksum` | Legacy field superseded by `worker_version`. Remains empty unless SDK sends it. | Intentionally permanent default — legacy field |
| `WorkflowTaskFailed` | `binary_checksum` | Same as above. | Intentionally permanent default — legacy field |
| `ActivityTaskStarted` | `worker_version` | Requires worker version reporting infrastructure. | `worker-heartbeat-observability` + `worker-deployments` |
| `ActivityTaskCompleted` | `worker_version` | Same. | `worker-heartbeat-observability` + `worker-deployments` |
| `ActivityTaskFailed` | `worker_version` | Same. | `worker-heartbeat-observability` + `worker-deployments` |

### Pause/Unpause Encoding

**Current behavior:** Maps to `WorkflowExecutionCanceledEventAttributes` — misleads SDK consumers into thinking the workflow was canceled.

**Chosen fix:** Encode as `MarkerRecordedEventAttributes` with a stable Tokeira marker name.

```rust
HistoryEventKind::WorkflowExecutionPaused { identity, reason, .. } => {
    Attributes::MarkerRecordedEventAttributes(
        history::MarkerRecordedEventAttributes {
            marker_name: "tokeira:paused".to_string(),
            details: /* encode identity + reason as marker details */,
            ..Default::default()
        },
    )
}
```

Rationale:
- Markers are opaque to SDK replay — they don't affect state-machine decisions
- SDKs skip unknown markers gracefully
- The marker name `tokeira:paused` / `tokeira:unpaused` is stable and greppable
- No risk of SDK misinterpreting the event as a cancellation

### Changes Required

**File**: `crates/tokeira-kernel/src/event.rs`
1. Add `workflow_task_completed_event_id: i64` to ~15 `HistoryEventKind` variants
2. Add enrichment fields per Class 2 table above
3. Add `history_size_bytes: i64` and `suggest_continue_as_new: bool` to `WorkflowTaskStarted`
4. Add `scheduled_event_id: i64` to `ActivityTaskCancelRequested`
5. Add `started_event_id: i64` to `TimerCanceled`

**File**: `crates/tokeira-kernel/src/lib.rs` (or command processing)
1. Thread `wft_completed_event_id` parameter through command application
2. Accept pre-resolved `activity_id → scheduled_event_id` and `timer_id → started_event_id` mappings

**File**: `crates/tokeira-runtime/src/lane.rs` (or equivalent)
1. Pass `wft_completed_event_id` when calling kernel command application
2. Provide activity/timer registry lookups
3. Stamp `history_size_bytes` on `WorkflowTaskStarted` events

**File**: `crates/tokeira-edge/src/translate/history_serializer.rs`
1. Wire through all Class 1 fields (remove `_` bindings, populate proto fields)
2. Wire through all Class 2 fields (read new kernel event fields, populate proto)
3. Wire through all Class 3 fields (read stamped event fields, populate proto)
4. Replace pause/unpause placeholder encoding with marker encoding
5. Add `workflow_task_completed_event_id` to all affected proto attribute structs

## Testing Strategy

### Validation Approach

The testing strategy follows a two-phase approach: first, surface counterexamples that demonstrate the bug on unfixed code, then verify the fix works correctly and preserves existing behavior.

### Exploratory Bug Condition Checking

**Goal**: Surface counterexamples that demonstrate the bug BEFORE implementing the fix. Confirm or refute the root cause analysis.

**Test Plan**: Write tests that construct kernel events with known field values and serialize them, then assert that specific proto fields contain the expected values. Run on UNFIXED code to observe failures.

**Test Cases**:
1. **Nexus operation_id discarded**: Construct `NexusOperationCompleted` with `operation_id: "op-123"`, serialize, decode proto, assert `operation_id` field is `"op-123"` (will fail — currently empty)
2. **ContinuedAsNew timeout discarded**: Construct `WorkflowExecutionContinuedAsNew` with `workflow_execution_timeout: Some(1h)`, serialize, assert proto field is populated (will fail — currently None)
3. **Pause misleading encoding**: Construct `WorkflowExecutionPaused`, serialize, assert event_type is NOT `WorkflowExecutionCanceled` (will fail — currently maps to canceled)
4. **workflow_task_completed_event_id zero**: Construct `WorkflowExecutionCompleted`, serialize, assert `workflow_task_completed_event_id > 0` (will fail — field doesn't exist on event yet)

**Expected Counterexamples**:
- Proto fields are zero/empty where kernel data exists
- Possible causes: `_` bindings, missing kernel fields, placeholder encodings

### Fix Checking

**Goal**: Verify that for all inputs where the bug condition holds, the fixed function produces the expected behavior.

**Pseudocode:**
```
FOR ALL event WHERE isBugCondition(event, field) DO
  result := serialize_fixed(event)
  ASSERT result.getField(field) == resolveAuthoritativeValue(event, field)
END FOR
```

### Preservation Checking

**Goal**: Verify that for all inputs where the bug condition does NOT hold, the fixed function produces the same result as the original function.

**Pseudocode:**
```
FOR ALL event WHERE NOT isBugCondition(event, field) DO
  ASSERT serialize_original(event) == serialize_fixed(event)
END FOR
```

**Testing Approach**: Property-based testing is recommended for preservation checking because:
- The existing `proptest` infrastructure in `history_serializer.rs` already generates arbitrary `HistoryEventKind` variants
- It catches edge cases across the full input domain
- It provides strong guarantees that existing correct behavior is unchanged

**Test Plan**: Extend the existing `arb_history_event_kind()` strategy to include new fields, then verify that all currently-correct fields produce identical output.

**Test Cases**:
1. **Round-trip preservation**: For all event kinds, verify `serialize_history` produces valid proto bytes that decode without error (existing test, must continue passing)
2. **Field stability**: For events with already-correct fields (workflow_type, task_queue, input, result, failure, etc.), verify the fixed serializer produces byte-identical output
3. **Default preservation**: For intentionally-defaulted fields, verify they remain default after the fix
4. **Deprecated field preservation**: Verify v0.4 wire-compat fields continue to be populated

### Unit Tests

- Test each Class 1 fix: construct event with the field, serialize, verify proto field is populated
- Test each Class 2 fix: construct enriched event, serialize, verify proto field is populated
- Test Class 3 `workflow_task_completed_event_id`: construct event with stamped ID, serialize, verify
- Test pause/unpause marker encoding: verify marker_name, details structure, and event_type
- Test edge cases: None/empty optional fields remain default in proto

### Property-Based Tests

- Extend `arb_history_event_kind()` to generate events with new enrichment fields
- Property: for any event, `serialize_history` produces bytes decodable as `History` (existing)
- Property: for any event with `workflow_task_completed_event_id > 0`, the proto field matches
- Property: for any Nexus terminal event, proto `operation_id` matches kernel `operation_id`
- Property: for any event, all fields listed in requirements 3.1–3.11 produce values matching the current (correct) serializer output

### Integration Tests

- End-to-end: start a workflow, complete a WFT with commands, read history via edge, verify `workflow_task_completed_event_id` is populated on command-produced events
- Pause/unpause: pause a workflow, read history, verify no `WorkflowExecutionCanceled` event type appears
- Child workflow: start and complete a child, verify parent history has namespace and run_id on terminal events
- Nexus: schedule and complete a Nexus operation, verify `operation_id` on completion event
