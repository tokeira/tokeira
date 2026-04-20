# Design Document: Edge Failure Object Completeness

## Overview

This design replaces bare `message: String` / `failure: String` fields on failure-bearing kernel events and commands with opaque `failure: Payload` fields that carry the full proto-encoded `temporal.api.failure.v1.Failure` bytes. The kernel never inspects the proto schema — it treats the failure as an opaque blob. The edge layer owns serialization (inbound: proto `Failure` → `Payload` via `failure_to_payload`) and deserialization (outbound: `Payload` → proto `Failure` via `payload_to_failure`).

This preserves all proto `Failure` fields — `failure_info` variants, `cause` chains, `stack_trace`, `source`, `encoded_attributes` — without the kernel needing to understand the proto `Failure` schema. The existing `failure_to_payload` and `payload_to_failure` helpers in `grpc/translate.rs` already implement the encoding/decoding; this design threads the opaque `Payload` through the kernel instead of decomposing it.

## Architecture

The data flow follows the standard translation pipeline, with the key change being that failures flow as opaque `Payload` blobs through the kernel:

```
┌─────────────────────────────────────────────────────────────────┐
│  SDK / gRPC Client                                               │
│  Sends proto Failure with all fields populated                   │
└──────────────────────────────┬──────────────────────────────────┘
                               │ proto Failure
┌──────────────────────────────▼──────────────────────────────────┐
│  Edge Layer — Inbound (grpc/translate.rs)                        │
│  failure_to_payload(proto_failure) → Payload                     │
│  Encodes entire Failure as opaque bytes with                     │
│  metadata: { encoding: "temporal/failure+proto" }                │
└──────────────────────────────┬──────────────────────────────────┘
                               │ Payload (opaque blob)
┌──────────────────────────────▼──────────────────────────────────┐
│  Kernel (pure state machine)                                     │
│  Stores failure: Payload in events and commands                  │
│  Never inspects the Payload contents                             │
└──────────────────────────────┬──────────────────────────────────┘
                               │ Payload (opaque blob)
┌──────────────────────────────▼──────────────────────────────────┐
│  Edge Layer — Outbound (history_serializer.rs)                   │
│  payload_to_failure(payload) → proto Failure                     │
│  Deserializes back to complete proto Failure                     │
└──────────────────────────────┬──────────────────────────────────┘
                               │ proto Failure (all fields intact)
┌──────────────────────────────▼──────────────────────────────────┐
│  SDK / gRPC Client                                               │
│  Receives proto Failure with failure_info, cause, stack_trace    │
└─────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### Component 1: Kernel event model — Replace bare strings with opaque Payload

**Problem:** Six `HistoryEventKind` variants carry failure information as bare strings, losing all structured proto `Failure` fields.

**Design:**

Replace the bare string fields with `failure: Payload` on each variant:

```rust
// Before:
WorkflowExecutionFailed { message: String, details: Option<Payload>, retry_state: RetryState, attempt: u32 }
ActivityTaskFailed { activity_id: String, scheduled_event_id: i64, started_event_id: i64, message: String }
ChildWorkflowExecutionFailed { child_workflow_id: WorkflowId, failure: String }
NexusOperationFailed { operation_id: String, scheduled_event_id: i64, failure: String }
WorkflowExecutionUpdateRejected { update_id: String, failure: String }

// After:
WorkflowExecutionFailed { failure: Payload, retry_state: RetryState, attempt: u32 }
ActivityTaskFailed { activity_id: String, scheduled_event_id: i64, started_event_id: i64, failure: Payload }
ChildWorkflowExecutionFailed { child_workflow_id: WorkflowId, failure: Payload }
NexusOperationFailed { operation_id: String, scheduled_event_id: i64, failure: Payload }
WorkflowExecutionUpdateRejected { update_id: String, failure: Payload }
```

Note: `WorkflowTaskFailed` already carries `failure_details: Option<Payload>` which is the correct shape. No change needed to its event variant.

Note: `WorkflowExecutionFailed` drops the `details: Option<Payload>` field because `encoded_attributes` is now preserved inside the opaque `Failure` bytes. The `message` field is also subsumed — it lives inside the proto `Failure.message`.

**Files changed:**
- `crates/tokeira-kernel/src/event.rs` — modify 5 `HistoryEventKind` variants, modify `ActivityResolution::Failed`
- `crates/tokeira-kernel/src/state.rs` — change `close_failure: Option<String>` to `close_failure: Option<Payload>` on `WorkflowState`

### Component 2: Kernel command model — Replace bare strings with opaque Payload

**Problem:** Four command/resolution types carry failure information as bare strings.

**Design:**

```rust
// Before:
WorkflowCommand::FailWorkflow { message: String, details: Option<Payload> }
ActivityResolution::Failed { message: String }
ChildResolution::Failed { failure: String }
NexusResolution::Failed { failure: String }
UpdateProtocolBody::Rejected { update_id: String, failure: String }
WorkflowCommand::UpdateRejected { update_id: String, failure: String }

// After:
WorkflowCommand::FailWorkflow { failure: Payload }
ActivityResolution::Failed { failure: Payload }
ChildResolution::Failed { failure: Payload }
NexusResolution::Failed { failure: Payload }
UpdateProtocolBody::Rejected { update_id: String, failure: Payload }
WorkflowCommand::UpdateRejected { update_id: String, failure: Payload }
```

**Files changed:**
- `crates/tokeira-kernel/src/command.rs` — modify `FailWorkflow`, `UpdateRejected`, `UpdateProtocolBody::Rejected`, `ChildResolution::Failed`, `NexusResolution::Failed`
- `crates/tokeira-kernel/src/event.rs` — modify `ActivityResolution::Failed`

### Component 3: Kernel apply methods — Thread opaque Payload

**Problem:** The kernel's `apply_*` methods currently extract `message` from commands and store it in events. They need to thread the opaque `Payload` instead.

**Design:**

In `kernel.rs`, each apply method that handles a failure path simply copies the `failure: Payload` from the command/resolution into the event. The kernel never inspects the `Payload` contents.

- `apply_workflow_task_completed` → when processing `WorkflowCommand::FailWorkflow { failure }`, emit `WorkflowExecutionFailed { failure, retry_state, attempt }`
- `apply_activity_resolved` → when processing `ActivityResolution::Failed { failure }`, emit `ActivityTaskFailed { ..., failure }`
- `apply_child_resolved` → when processing `ChildResolution::Failed { failure }`, emit `ChildWorkflowExecutionFailed { ..., failure }`
- `apply_nexus_operation_resolved` → when processing `NexusResolution::Failed { failure }`, emit `NexusOperationFailed { ..., failure }`
- `apply_workflow_task_completed` → when processing `WorkflowCommand::UpdateRejected { update_id, failure }`, emit `WorkflowExecutionUpdateRejected { update_id, failure }`

**Files changed:**
- `crates/tokeira-kernel/src/kernel.rs` — update all apply methods that handle failure paths

### Component 4: Edge inbound translation — Encode full Failure as Payload

**Problem:** The inbound translation currently decomposes proto `Failure` into bare strings. It needs to encode the entire `Failure` as an opaque `Payload`.

**Design:**

**FailWorkflow command translation** (`proto_command_to_workflow_command`):

```rust
// Before:
Some(Attributes::FailWorkflowExecutionCommandAttributes(attrs)) => {
    let message = attrs.failure.as_ref().map(|f| f.message.clone()).unwrap_or_default();
    Ok(WorkflowCommand::FailWorkflow { message, details: None })
}

// After:
Some(Attributes::FailWorkflowExecutionCommandAttributes(attrs)) => {
    let failure = attrs.failure.as_ref()
        .map(failure_to_payload)
        .unwrap_or_else(|| failure_to_payload(&failure_proto::Failure::default()));
    Ok(WorkflowCommand::FailWorkflow { failure })
}
```

**Activity fail translation** (`respond_activity_failed_to_edge`):

```rust
// Before:
let (failure_message, failure_error_type) = match req.failure {
    Some(f) => (f.message, non_empty(f.source)),
    None => (String::new(), None),
};
Ok(RespondActivityTaskFailedRequest { token, failure_message, failure_error_type, identity })

// After:
let failure = req.failure.as_ref()
    .map(failure_to_payload)
    .unwrap_or_else(|| failure_to_payload(&failure_proto::Failure::default()));
Ok(RespondActivityTaskFailedRequest { token, failure, identity })
```

**Update rejection translation** (`resolve_protocol_message_body`):

```rust
// Before (Rejection):
Ok(UpdateProtocolBody::Rejected {
    update_id: protocol_instance_id,
    failure: rejection.failure.map(|f| f.message).unwrap_or_else(|| "update rejected".to_string()),
})

// After:
Ok(UpdateProtocolBody::Rejected {
    update_id: protocol_instance_id,
    failure: rejection.failure.as_ref()
        .map(failure_to_payload)
        .unwrap_or_else(|| failure_to_payload(&failure_proto::Failure {
            message: "update rejected".to_string(),
            ..Default::default()
        })),
})
```

Similarly for the `Response` with `Failure` outcome path.

**`workflow_command_to_proto` (reverse direction for FailWorkflow):**

```rust
// Before:
WorkflowCommand::FailWorkflow { message, details: _ } => {
    Some(Attributes::FailWorkflowExecutionCommandAttributes(
        command::FailWorkflowExecutionCommandAttributes {
            failure: Some(failure_proto::Failure { message: message.clone(), ..Default::default() }),
        },
    ))
}

// After:
WorkflowCommand::FailWorkflow { failure } => {
    Some(Attributes::FailWorkflowExecutionCommandAttributes(
        command::FailWorkflowExecutionCommandAttributes {
            failure: Some(payload_to_failure(failure)),
        },
    ))
}
```

**Files changed:**
- `crates/tokeira-edge/src/grpc/translate.rs` — update `proto_command_to_workflow_command`, `workflow_command_to_proto`, `respond_activity_failed_to_edge`, `resolve_protocol_message_body`

### Component 5: Edge DTO — Replace bare strings with opaque Payload

**Problem:** `RespondActivityTaskFailedRequest` carries `failure_message: String` and `failure_error_type: Option<String>`.

**Design:**

```rust
// Before:
pub struct RespondActivityTaskFailedRequest {
    pub token: ActivityTaskToken,
    pub failure_message: String,
    pub failure_error_type: Option<String>,
    pub identity: String,
}

// After:
pub struct RespondActivityTaskFailedRequest {
    pub token: ActivityTaskToken,
    pub failure: Payload,
    pub identity: String,
}
```

**Files changed:**
- `crates/tokeira-edge/src/translate/mod.rs` — modify `RespondActivityTaskFailedRequest`

### Component 6: Runtime — Thread opaque Payload through activity fail path

**Problem:** `fail_activity_task` currently takes `failure_message: String` and `failure_error_type: Option<String>`, decomposing the failure. It needs to take `failure: Payload` and pass it through.

**Design:**

```rust
// Before:
pub async fn fail_activity_task(
    &self,
    token: ActivityTaskToken,
    failure_message: String,
    failure_error_type: Option<String>,
) -> Result<()> {
    // ... retry logic uses failure_error_type ...
    resolution: ActivityResolution::Failed { message: failure_message },
}

// After:
pub async fn fail_activity_task(
    &self,
    token: ActivityTaskToken,
    failure: Payload,
    failure_error_type: Option<String>,
) -> Result<()> {
    // ... retry logic still uses failure_error_type for retry decisions ...
    resolution: ActivityResolution::Failed { failure },
}
```

The `failure_error_type` parameter is still needed for retry decisions. However, the current code incorrectly extracts it from `Failure.source` — that field identifies the SDK/server origin (e.g., "GoSDK"), not the application error type. The Temporal server's `isRetryable` function uses `ApplicationFailureInfo.type` for matching against `non_retryable_error_types`, and also checks `ApplicationFailureInfo.non_retryable`, `ServerFailureInfo.non_retryable`, `TerminatedFailureInfo`, `CanceledFailureInfo`, and `TimeoutFailureInfo.timeout_type`.

The edge layer SHALL extract retry-relevant classification from the proto `Failure` before encoding it as a `Payload`:

```rust
/// Extract retry classification from a proto Failure.
/// Returns (application_error_type, is_non_retryable).
fn extract_retry_classification(f: &failure_proto::Failure) -> (Option<String>, bool) {
    match &f.failure_info {
        Some(FailureInfo::ApplicationFailureInfo(info)) => {
            (non_empty(info.r#type.clone()), info.non_retryable)
        }
        Some(FailureInfo::ServerFailureInfo(info)) => {
            (None, info.non_retryable)
        }
        Some(FailureInfo::TerminatedFailureInfo(_))
        | Some(FailureInfo::CanceledFailureInfo(_)) => {
            (None, true)  // always non-retryable
        }
        Some(FailureInfo::TimeoutFailureInfo(info)) => {
            let timeout_type = info.timeout_type;
            // START_TO_CLOSE and HEARTBEAT are retryable (unless in non_retryable_error_types)
            let is_retryable_timeout = timeout_type == 3 || timeout_type == 4;
            if is_retryable_timeout {
                (Some(format!("Timeout:{timeout_type}")), false)
            } else {
                (None, true)
            }
        }
        _ => (None, false),
    }
}
```

Updated edge DTO:

```rust
pub struct RespondActivityTaskFailedRequest {
    pub token: ActivityTaskToken,
    pub failure: Payload,
    pub failure_error_type: Option<String>,
    pub is_non_retryable: bool,
    pub identity: String,
}
```

Updated `respond_activity_failed_to_edge`:

```rust
let (failure_error_type, is_non_retryable) = req.failure.as_ref()
    .map(extract_retry_classification)
    .unwrap_or((None, false));
let failure = req.failure.as_ref()
    .map(failure_to_payload)
    .unwrap_or_else(|| failure_to_payload(&failure_proto::Failure::default()));
Ok(RespondActivityTaskFailedRequest { token, failure, failure_error_type, is_non_retryable, identity })
```

**Files changed:**
- `crates/tokeira-runtime/src/runtime.rs` — update `fail_activity_task` signature and body
- `crates/tokeira-edge/src/workflow_service.rs` — update call site to pass `failure`, `failure_error_type`, and `is_non_retryable`
- `crates/tokeira-edge/src/translate/mod.rs` — update `RespondActivityTaskFailedRequest`

### Component 6b: Shared failure encoding utility

**Problem:** The `failure_to_payload` and `payload_to_failure` helpers currently live in `tokeira-edge/src/grpc/translate.rs`, which is private to the edge crate. Tasks 5b.1 (child resolution in `lane.rs`) and 5b.2 (Nexus publisher) need to construct opaque failure payloads from the runtime crate, which cannot depend on the edge crate.

**Design:**

Move `failure_to_payload` and `payload_to_failure` to `tokeira-proto/src/conversions/common.rs` (or a new `failure.rs` sub-module). The proto crate is already a dependency of both the edge and runtime crates, making it the natural shared location. Re-export from `tokeira_proto::conversions::common` and update all import sites.

**Files changed:**
- `crates/tokeira-proto/src/conversions/common.rs` — add `failure_to_payload` and `payload_to_failure`
- `crates/tokeira-edge/src/grpc/translate.rs` — remove local helpers, import from `tokeira_proto`
- `crates/tokeira-runtime/Cargo.toml` — add `tokeira-proto` dependency if not present

### Component 7: History serializer — Deserialize opaque Payload back to proto Failure

**Problem:** The history serializer currently constructs `Failure` objects from bare strings, losing all structured fields. It needs to deserialize the opaque `Payload` back to a complete proto `Failure`.

**Design:**

Add a helper function that converts an opaque `Payload` to a proto `Failure`:

```rust
fn failure_payload_to_proto(payload: &Payload) -> proto_failure::Failure {
    // Check for the temporal/failure+proto encoding marker
    if payload.metadata.get("encoding").map(|e| e.as_str()) == Some("temporal/failure+proto") {
        proto_failure::Failure::decode(payload.data.as_slice()).unwrap_or_else(|_| {
            proto_failure::Failure {
                message: String::from_utf8_lossy(&payload.data).into_owned(),
                ..Default::default()
            }
        })
    } else {
        // Legacy or unknown encoding — treat data as message
        proto_failure::Failure {
            message: String::from_utf8_lossy(&payload.data).into_owned(),
            ..Default::default()
        }
    }
}
```

Then update each failure-bearing event arm:

**WorkflowExecutionFailed:**
```rust
HistoryEventKind::WorkflowExecutionFailed { failure, retry_state, attempt: _ } => {
    Attributes::WorkflowExecutionFailedEventAttributes(
        history::WorkflowExecutionFailedEventAttributes {
            failure: Some(failure_payload_to_proto(failure)),
            retry_state: retry_state_i32(retry_state),
            ..Default::default()
        },
    )
}
```

**ActivityTaskFailed:**
```rust
HistoryEventKind::ActivityTaskFailed { activity_id: _, scheduled_event_id, started_event_id, failure } => {
    Attributes::ActivityTaskFailedEventAttributes(
        history::ActivityTaskFailedEventAttributes {
            failure: Some(failure_payload_to_proto(failure)),
            scheduled_event_id: *scheduled_event_id,
            started_event_id: *started_event_id,
            ..Default::default()
        },
    )
}
```

**ChildWorkflowExecutionFailed:**
```rust
HistoryEventKind::ChildWorkflowExecutionFailed { child_workflow_id, failure } => {
    Attributes::ChildWorkflowExecutionFailedEventAttributes(
        history::ChildWorkflowExecutionFailedEventAttributes {
            failure: Some(failure_payload_to_proto(failure)),
            workflow_execution: Some(proto_common::WorkflowExecution {
                workflow_id: child_workflow_id.0.clone(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
}
```

**WorkflowTaskFailed** (fix existing — currently constructs empty Failure):
```rust
HistoryEventKind::WorkflowTaskFailed { ..., failure_details, ... } => {
    let failure = failure_details.as_ref().map(failure_payload_to_proto);
    Attributes::WorkflowTaskFailedEventAttributes(
        history::WorkflowTaskFailedEventAttributes {
            failure,
            // ... other fields ...
        },
    )
}
```

**NexusOperationFailed:**
```rust
HistoryEventKind::NexusOperationFailed { operation_id: _, scheduled_event_id, failure } => {
    Attributes::NexusOperationFailedEventAttributes(
        history::NexusOperationFailedEventAttributes {
            scheduled_event_id: *scheduled_event_id,
            failure: Some(failure_payload_to_proto(failure)),
            ..Default::default()
        },
    )
}
```

**WorkflowExecutionUpdateRejected:**
```rust
HistoryEventKind::WorkflowExecutionUpdateRejected { update_id, failure } => {
    Attributes::WorkflowExecutionUpdateRejectedEventAttributes(
        history::WorkflowExecutionUpdateRejectedEventAttributes {
            protocol_instance_id: update_id.clone(),
            failure: Some(failure_payload_to_proto(failure)),
            ..Default::default()
        },
    )
}
```

**MarkerRecorded** (fix existing — currently constructs empty Failure):
```rust
failure: failure.as_ref().map(failure_payload_to_proto),
```

**Files changed:**
- `crates/tokeira-edge/src/translate/history_serializer.rs` — add `failure_payload_to_proto` helper, update 7 event arms

### Component 8: Fix downstream compilation

**Problem:** Changing the field types on kernel events and commands will break pattern matches and construction sites across the codebase.

**Design:**

All pattern matches on the modified variants need updating:
- `kernel.rs` — apply methods that emit or match failure events
- `history_serializer.rs` — already covered by Component 7
- `property_tests.rs` — proptest generators for events and resolutions
- `grpc/translate.rs` — already covered by Component 4
- `workflow_service.rs` — call sites for `fail_activity_task`
- `runtime.rs` — `fail_activity_task` signature and body

The proptest generators need to produce `Payload` values with the `temporal/failure+proto` encoding. A helper `arb_failure_payload()` generates arbitrary proto `Failure` objects, encodes them via `failure_to_payload`, and returns the `Payload`.

**Files changed:**
- `crates/tokeira-kernel/tests/property_tests.rs` — update generators and assertions
- `crates/tokeira-edge/src/translate/history_serializer.rs` — update proptest generators in `mod tests`
- All other files with pattern matches on the modified variants

## Data Models

### Modified: `HistoryEventKind` (kernel)

```rust
WorkflowExecutionFailed {
    failure: Payload,           // WAS: message: String, details: Option<Payload>
    retry_state: RetryState,
    attempt: u32,
}

ActivityTaskFailed {
    activity_id: String,
    scheduled_event_id: i64,
    started_event_id: i64,
    failure: Payload,           // WAS: message: String
}

ChildWorkflowExecutionFailed {
    child_workflow_id: WorkflowId,
    failure: Payload,           // WAS: failure: String
}

NexusOperationFailed {
    operation_id: String,
    scheduled_event_id: i64,
    failure: Payload,           // WAS: failure: String
}

WorkflowExecutionUpdateRejected {
    update_id: String,
    failure: Payload,           // WAS: failure: String
}
```

### Modified: `ActivityResolution` (kernel)

```rust
pub enum ActivityResolution {
    Completed { result: Payloads },
    Failed { failure: Payload },    // WAS: message: String
    TimedOut { timeout_type: String },
    Canceled { details: Option<Payloads> },
}
```

### Modified: `WorkflowCommand` (kernel)

```rust
WorkflowCommand::FailWorkflow {
    failure: Payload,               // WAS: message: String, details: Option<Payload>
}

WorkflowCommand::UpdateRejected {
    update_id: String,
    failure: Payload,               // WAS: failure: String
}
```

### Modified: `UpdateProtocolBody` (kernel)

```rust
UpdateProtocolBody::Rejected {
    update_id: String,
    failure: Payload,               // WAS: failure: String
}
```

### Modified: `ChildResolution` (kernel)

```rust
ChildResolution::Failed {
    failure: Payload,               // WAS: failure: String
}
```

### Modified: `NexusResolution` (kernel)

```rust
NexusResolution::Failed {
    failure: Payload,               // WAS: failure: String
}
```

### Modified: `RespondActivityTaskFailedRequest` (edge DTO)

```rust
pub struct RespondActivityTaskFailedRequest {
    pub token: ActivityTaskToken,
    pub failure: Payload,           // WAS: failure_message: String
    pub failure_error_type: Option<String>,  // KEPT for retry decisions
    pub identity: String,
}
```

## Correctness Properties

### Property 1: failure_to_payload / payload_to_failure round-trip

*For any* proto `Failure` with arbitrary fields populated (message, source, stack_trace, encoded_attributes, cause chain, failure_info variant), encoding via `failure_to_payload` then decoding via `payload_to_failure` SHALL produce a proto `Failure` that is byte-identical to the original when re-encoded.

**Validates:** Requirement 8, AC 8.1

### Property 2: WorkflowExecutionFailed history serializer round-trip

*For any* `HistoryEvent` with kind `WorkflowExecutionFailed` where `failure` is an Opaque_Failure_Payload containing a proto `Failure` with `failure_info`, `cause`, `stack_trace`, and `encoded_attributes`, serializing via `history_event_to_proto` SHALL produce a `WorkflowExecutionFailedEventAttributes` whose `failure` field contains a proto `Failure` with all original fields preserved.

**Validates:** Requirement 1 (AC 1.4), Requirement 8 (AC 8.2)

### Property 3: ActivityTaskFailed history serializer round-trip

*For any* `HistoryEvent` with kind `ActivityTaskFailed` where `failure` is an Opaque_Failure_Payload containing a proto `Failure` with `failure_info` and `cause`, serializing via `history_event_to_proto` SHALL produce an `ActivityTaskFailedEventAttributes` whose `failure` field contains a proto `Failure` with all original fields preserved.

**Validates:** Requirement 2 (AC 2.6), Requirement 8 (AC 8.3)

### Property 4: ChildWorkflowExecutionFailed history serializer round-trip

*For any* `HistoryEvent` with kind `ChildWorkflowExecutionFailed` where `failure` is an Opaque_Failure_Payload, serializing via `history_event_to_proto` SHALL produce a `ChildWorkflowExecutionFailedEventAttributes` whose `failure` field contains a proto `Failure` with all original fields preserved.

**Validates:** Requirement 3 (AC 3.3), Requirement 8 (AC 8.4)

### Property 5: WorkflowTaskFailed history serializer round-trip

*For any* `HistoryEvent` with kind `WorkflowTaskFailed` where `failure_details` is `Some(Payload)` containing a proto `Failure`, serializing via `history_event_to_proto` SHALL produce a `WorkflowTaskFailedEventAttributes` whose `failure` field contains a proto `Failure` with all original fields preserved.

**Validates:** Requirement 4 (AC 4.2), Requirement 8 (AC 8.5)

### Property 6: All failure-bearing events preserve failure_info

*For any* `HistoryEvent` with a failure-bearing kind (WorkflowExecutionFailed, ActivityTaskFailed, ChildWorkflowExecutionFailed, NexusOperationFailed, WorkflowExecutionUpdateRejected, MarkerRecorded with failure), where the failure Payload encodes a proto `Failure` with a non-None `failure_info`, serializing via `history_event_to_proto` SHALL produce proto attributes whose `failure` field has a non-None `failure_info`.

**Validates:** Requirements 1-7

## Error Handling

No new error paths are introduced. The changes replace string extraction with opaque blob threading:

- If `failure_to_payload` receives a `Failure` with default fields, it produces a valid `Payload` with empty proto bytes — this is correct behavior (an empty failure is still a valid failure).
- If `payload_to_failure` receives corrupted bytes, it falls back to interpreting the bytes as a UTF-8 message string. This matches the existing behavior in `grpc/translate.rs`.
- If `failure_payload_to_proto` in the history serializer receives a `Payload` without the `temporal/failure+proto` encoding marker, it falls back to treating the data as a message string. This handles legacy data gracefully.
- The `failure_error_type` extraction for activity retry decisions happens at the edge layer before encoding, so retry logic is unaffected.

## Testing Strategy

### Property-based tests (proptest, 100 iterations)

1. **failure_to_payload / payload_to_failure round-trip** — Generate arbitrary proto `Failure` objects with random `message`, `source`, `stack_trace`, `encoded_attributes`, `cause` chains (depth 0-3), and `failure_info` variants. Encode via `failure_to_payload`, decode via `payload_to_failure`, assert the decoded `Failure` re-encodes to identical bytes. (Property 1)

2. **WorkflowExecutionFailed serialization** — Generate arbitrary `HistoryEvent` values with `WorkflowExecutionFailed` kind carrying a full Opaque_Failure_Payload. Serialize to proto via `history_event_to_proto`. Assert the proto `failure` field contains a `Failure` with non-None `failure_info` when the input had one. (Property 2)

3. **ActivityTaskFailed serialization** — Same pattern for `ActivityTaskFailed` events. (Property 3)

4. **ChildWorkflowExecutionFailed serialization** — Same pattern for `ChildWorkflowExecutionFailed` events. (Property 4)

5. **WorkflowTaskFailed serialization** — Generate `WorkflowTaskFailed` events with `failure_details: Some(Payload)`. Assert the proto `failure` field is populated (not None) and contains the original `failure_info`. (Property 5)

6. **All failure-bearing events preserve failure_info** — Generate events across all failure-bearing kinds. Assert `failure_info` is preserved through serialization. (Property 6)

### Unit tests (example-based)

- History serializer: `WorkflowExecutionFailed` with `ApplicationFailureInfo` produces proto with `failure_info` populated
- History serializer: `ActivityTaskFailed` with `ApplicationFailureInfo` and `cause` chain produces proto with both fields
- History serializer: `WorkflowTaskFailed` with `failure_details` produces proto with non-empty `Failure` (regression for current bug)
- History serializer: `MarkerRecorded` with failure produces proto with non-empty `Failure` (regression for current bug)
- Edge: `proto_command_to_workflow_command` for `FailWorkflow` produces `Payload` with `temporal/failure+proto` encoding
- Edge: `respond_activity_failed_to_edge` produces `Payload` with `temporal/failure+proto` encoding
- Fallback: corrupted `Payload` data produces a `Failure` with the raw bytes as message
