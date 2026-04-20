# Design Document: Edge History Parent Chain

## Overview

This design threads missing parent metadata, execution chain fields, and continue-as-new state through the kernel event model, runtime, and history serializer. The kernel remains pure — all new data enters via `StartRequest` or `ContinueAsNew` command fields. The runtime is responsible for reading predecessor state and threading it into commands. The history serializer maps the enriched kernel events to complete proto attributes.

The work is organized into seven components:
1. Kernel event model — enrich `WorkflowExecutionStarted` with parent and chain fields
2. Kernel event model — enrich `WorkflowExecutionContinuedAsNew` with retry_policy, initiator, failure, last_completion_result
3. Kernel command model — enrich `StartRequest` and `ContinueAsNew` command
4. Kernel apply methods — thread new fields through state transitions
5. Runtime — thread parent metadata and predecessor state into commands
6. History serializer — populate proto fields from enriched events
7. Kernel event model — add `control` field to signal-external and cancel-external events

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Runtime (orchestration + I/O)                                   │
│  ─ publisher.rs: threads parent_namespace_id, parent_run_id,     │
│    parent_initiated_event_id into StartRequest for child starts  │
│  ─ lane.rs: reads predecessor close_failure, close_result,       │
│    original_execution_run_id for continue-as-new successors      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ StartRequest / ContinueAsNew command
┌──────────────────────────────▼──────────────────────────────────┐
│  Kernel (pure state machine)                                     │
│  ─ Stores new fields on WorkflowState                            │
│  ─ Emits enriched WorkflowExecutionStarted events                │
│  ─ Emits enriched WorkflowExecutionContinuedAsNew events         │
│  ─ Threads control field on signal/cancel-external events        │
└──────────────────────────────┬──────────────────────────────────┘
                               │ HistoryEvent
┌──────────────────────────────▼──────────────────────────────────┐
│  History Serializer (proto translation)                          │
│  ─ Populates parent_workflow_execution, parent_namespace,        │
│    parent_initiated_event_id, original_execution_run_id,         │
│    continued_failure, last_completion_result on Started proto     │
│  ─ Populates retry_policy, initiator,                            │
│    failure, last_completion_result on ContinuedAsNew proto       │
│  ─ Populates control on signal/cancel-external initiated protos  │
└─────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### Component 1: Kernel event model — Enrich `WorkflowExecutionStarted`

**Problem:** The `WorkflowExecutionStarted` event variant carries workflow_type, task_queue, input, etc. but is missing parent workflow metadata (`parent_workflow_execution`, `parent_namespace_id`, `parent_initiated_event_id`), execution chain fields (`original_execution_run_id`), and predecessor state (`continued_failure`, `last_completion_result`). The data exists on `StartRequest` and `WorkflowState` but is not threaded into the event.

**Design:**

Add six new fields to `HistoryEventKind::WorkflowExecutionStarted`:

```rust
WorkflowExecutionStarted {
    // ... existing fields ...
    workflow_type: WorkflowType,
    task_queue: TaskQueueName,
    input: Payloads,
    memo: Memo,
    search_attributes: SearchAttributes,
    request_id: String,
    continued_execution_run_id: Option<RunId>,
    first_execution_run_id: Option<RunId>,
    retry_policy: Option<RetryPolicy>,
    attempt: u32,
    workflow_execution_timeout: Option<Duration>,
    workflow_run_timeout: Option<Duration>,
    workflow_task_timeout: Duration,
    // NEW fields:
    parent_workflow_id: Option<WorkflowId>,
    parent_run_id: Option<RunId>,
    parent_namespace_id: Option<NamespaceId>,
    parent_initiated_event_id: i64,
    original_execution_run_id: Option<RunId>,
    continued_failure: Option<Payload>,
    last_completion_result: Option<Payloads>,
}
```

The kernel's `apply_start` method populates these from the `StartRequest`. For `original_execution_run_id`, if the `StartRequest` field is `None` (first run), the kernel sets it to `Some(req.run_id)`.

**Files changed:**
- `crates/tokeira-kernel/src/event.rs` — add 7 fields to `WorkflowExecutionStarted`

### Component 2: Kernel event model — Enrich `WorkflowExecutionContinuedAsNew`

**Problem:** The `WorkflowExecutionContinuedAsNew` event variant is missing `retry_policy`, `initiator`, `failure`, and `last_completion_result`. The `workflow_execution_timeout` field is correctly ignored with `_` because the upstream proto does not have this field.

**Design:**

Add four new fields and a new `ContinueAsNewInitiator` enum:

```rust
/// What triggered the continue-as-new.
#[derive(Clone, Debug, PartialEq)]
pub enum ContinueAsNewInitiator {
    /// The workflow itself issued a ContinueAsNew command.
    Workflow,
    /// The runtime retried a failed workflow via continue-as-new.
    Retry,
    /// A cron schedule triggered a new execution (deferred to Feature 6).
    CronSchedule,
}

WorkflowExecutionContinuedAsNew {
    // ... existing fields ...
    new_run_id: RunId,
    workflow_type: WorkflowType,
    task_queue: TaskQueueName,
    input: Payloads,
    memo: Memo,
    search_attributes: SearchAttributes,
    workflow_execution_timeout: Option<Duration>,
    workflow_run_timeout: Option<Duration>,
    workflow_task_timeout: Duration,
    // NEW fields:
    retry_policy: Option<RetryPolicy>,
    initiator: ContinueAsNewInitiator,
    failure: Option<Payload>,
    last_completion_result: Option<Payloads>,
}
```

The `ContinueAsNewInitiator` enum lives in `crates/tokeira-kernel/src/command.rs` alongside other kernel enums.

When the kernel processes a `ContinueAsNew` workflow command, it sets `initiator: ContinueAsNewInitiator::Workflow`. The `failure` and `last_completion_result` fields are `None` for workflow-initiated CAN (the run hasn't completed or failed — it's continuing).

Note: Retry-initiated continue-as-new does NOT produce a `WorkflowExecutionContinuedAsNew` event in the current architecture. When a failed run is retried, the runtime emits `WorkflowExecutionFailed` and creates a successor `StartRequest` directly. The retry failure and last completion result are carried on the successor's `WorkflowExecutionStarted` event via `continued_failure` and `last_completion_result` on `StartRequest`.

**Files changed:**
- `crates/tokeira-kernel/src/event.rs` — add 4 fields to `WorkflowExecutionContinuedAsNew`
- `crates/tokeira-kernel/src/command.rs` — add `ContinueAsNewInitiator` enum

### Component 3: Kernel command model — Enrich `StartRequest` and `ContinueAsNew`

**Problem:** `StartRequest` is missing `parent_namespace_id`, `parent_run_id`, `parent_initiated_event_id`, `original_execution_run_id`, `continued_failure`, and `last_completion_result`. The `ContinueAsNew` workflow command is missing `retry_policy`, `initiator`, `failure`, and `last_completion_result`.

**Design:**

Add fields to `StartRequest`:

```rust
pub struct StartRequest {
    // ... existing fields ...
    pub parent_run_key: Option<RunKey>,
    pub parent_workflow_id: Option<WorkflowId>,
    // NEW fields:
    pub parent_run_id: Option<RunId>,
    pub parent_namespace_id: Option<NamespaceId>,
    pub parent_initiated_event_id: i64,
    pub original_execution_run_id: Option<RunId>,
    pub continued_failure: Option<Payload>,
    pub last_completion_result: Option<Payloads>,
    // ... rest of existing fields ...
}
```

Add fields to `WorkflowCommand::ContinueAsNew`:

```rust
WorkflowCommand::ContinueAsNew {
    // ... existing fields ...
    new_run_id: RunId,
    workflow_type: WorkflowType,
    task_queue: TaskQueueName,
    input: Payloads,
    memo: Memo,
    search_attributes: SearchAttributes,
    workflow_execution_timeout: Option<Duration>,
    workflow_run_timeout: Option<Duration>,
    workflow_task_timeout: Duration,
    // NEW fields:
    retry_policy: Option<RetryPolicy>,
}
```

Note: `initiator`, `failure`, and `last_completion_result` are NOT on the `ContinueAsNew` command because the workflow command always has `initiator: Workflow` and no failure. For retry-initiated CAN, the runtime constructs the successor `StartRequest` directly — the kernel doesn't process a `ContinueAsNew` command for retries.

Add fields to `WorkflowState`:

```rust
pub struct WorkflowState {
    // ... existing fields ...
    pub original_execution_run_id: Option<RunId>,  // NEW
    pub parent_run_id: Option<RunId>,              // NEW
    pub parent_namespace_id: Option<NamespaceId>,  // NEW
    pub parent_initiated_event_id: i64,            // NEW (0 = no parent)
    pub last_completion_result: Option<Payloads>,  // NEW
}
```

Also add to `SignalWithStartRequest` the same new fields as `StartRequest` for consistency.

Add `control: String` to `WorkflowCommand::SignalExternalWorkflowExecution` and `WorkflowCommand::RequestCancelExternalWorkflowExecution`:

```rust
WorkflowCommand::SignalExternalWorkflowExecution {
    // ... existing fields ...
    control: String,  // NEW
}

WorkflowCommand::RequestCancelExternalWorkflowExecution {
    // ... existing fields ...
    control: String,  // NEW
}
```

**Files changed:**
- `crates/tokeira-kernel/src/command.rs` — add fields to `StartRequest`, `SignalWithStartRequest`, `ContinueAsNew`, `SignalExternalWorkflowExecution`, `RequestCancelExternalWorkflowExecution`
- `crates/tokeira-kernel/src/state.rs` — add fields to `WorkflowState`

### Component 4: Kernel apply methods — Thread new fields

**Problem:** The kernel's `apply_start`, `apply_signal_with_start`, `apply_workflow_task_completed` (ContinueAsNew arm), and `replay_history_prefix` methods need to thread the new fields.

**Design:**

**`apply_start`:** Populate the new `WorkflowState` fields from `StartRequest`. Emit the enriched `WorkflowExecutionStarted` event with all new fields. For `original_execution_run_id`, if `req.original_execution_run_id` is `None`, set it to `Some(req.run_id)`.

**`apply_signal_with_start`:** Same as `apply_start` — populate state and emit enriched event.

**`apply_workflow_task_completed` (ContinueAsNew arm):** When processing `WorkflowCommand::ContinueAsNew`, emit the enriched `WorkflowExecutionContinuedAsNew` event with:
- `retry_policy`: from the command's `retry_policy` field, falling back to `builder.state.retry_policy` if `None`
- `initiator`: `ContinueAsNewInitiator::Workflow` (always, for workflow-initiated CAN)
- `failure`: `None` (workflow-initiated CAN doesn't have a failure)
- `last_completion_result`: `None` (the current run hasn't completed yet — it's continuing)

**`apply_workflow_task_completed` (SignalExternalWorkflowExecution arm):** Thread `control` from the command into the `SignalExternalWorkflowExecutionInitiated` event.

**`apply_workflow_task_completed` (RequestCancelExternalWorkflowExecution arm):** Thread `control` from the command into the `RequestCancelExternalWorkflowExecutionInitiated` event.

**`replay_history_prefix`:** When replaying `WorkflowExecutionStarted`, extract the new fields and populate `WorkflowState`. When replaying `WorkflowExecutionContinuedAsNew`, handle the new fields in the match arm.

**Files changed:**
- `crates/tokeira-kernel/src/kernel.rs` — update `apply_start`, `apply_signal_with_start`, `apply_workflow_task_completed`, `apply_replayed_event`

### Component 5: Runtime — Thread parent metadata and predecessor state

**Problem:** The runtime's `handle_start_child_workflow` in `publisher.rs` doesn't populate `parent_run_id`, `parent_namespace_id`, or `parent_initiated_event_id` on the `StartRequest`. The runtime's continue-as-new path in `lane.rs` doesn't read the predecessor's `close_failure`, `close_result`, or `original_execution_run_id`.

**Design:**

**Child workflow start (`publisher.rs`):**

The `DispatchOp::StartChildWorkflow` already carries `parent_run_key`, `parent_workflow_id`, and `initiated_event_id`. We need to also carry `parent_namespace_id` and `parent_run_id` (the parent's actual `RunId`, not the `RunKey`).

Add `parent_run_id: RunId` and `parent_namespace_id: NamespaceId` to `DispatchOp::StartChildWorkflow`. The kernel populates these from `builder.state.run_id` and `builder.state.namespace_id` when emitting the dispatch op.

In `handle_start_child_workflow`, populate the new `StartRequest` fields:

```rust
let start_request = StartRequest {
    // ... existing fields ...
    parent_run_key: Some(parent_run_key),
    parent_workflow_id: Some(parent_workflow_id),
    parent_run_id: Some(parent_run_id),           // NEW
    parent_namespace_id: Some(namespace_id),       // NEW — parent's namespace
    parent_initiated_event_id: initiated_event_id, // NEW
    original_execution_run_id: None,               // child starts a new chain
    continued_failure: None,
    last_completion_result: None,
    // ...
};
```

**Continue-as-new successor (`lane.rs`):**

The lane already reads the `WorkflowExecutionContinuedAsNew` event to extract successor parameters. Extend this to also read from `new_state` (the predecessor's final `WorkflowState`):

```rust
let start_request = StartRequest {
    // ... existing fields ...
    original_execution_run_id: Some(
        new_state.original_execution_run_id.unwrap_or(new_state.run_id)
    ),
    continued_failure: new_state.close_failure.clone(),
    last_completion_result: new_state.close_result.clone().map(|r| r),
    parent_run_key: None,
    parent_workflow_id: None,
    parent_run_id: None,
    parent_namespace_id: None,
    parent_initiated_event_id: 0,
    // ...
};
```

Note: `close_failure` is `Option<Payload>` and `close_result` is `Option<Payloads>` on `WorkflowState`. For continue-as-new, the predecessor's `close_failure` is set if the run failed (retry-initiated CAN). For workflow-initiated CAN, `close_failure` is `None` and `close_result` is `None` (the run didn't complete — it continued).

For `last_completion_result` to work correctly across chains, the `WorkflowState` needs to track the last completion result from the chain. This is populated from the `StartRequest.last_completion_result` (propagated from the predecessor) and updated when the current run completes successfully. However, for the initial implementation, we only need to propagate `close_result` from the immediate predecessor — the SDK handles chain tracking.

**Edge inbound translation (`grpc/translate.rs`):**

The `proto_command_to_workflow_command` function for `ContinueAsNewWorkflowExecutionCommandAttributes` needs to extract `retry_policy` from the proto command and pass it to the kernel command. The `control` field needs to be extracted from `SignalExternalWorkflowExecutionCommandAttributes` and `RequestCancelExternalWorkflowExecutionCommandAttributes`.

**Files changed:**
- `crates/tokeira-kernel/src/transition.rs` — add `parent_run_id: RunId` and `parent_namespace_id: NamespaceId` to `DispatchOp::StartChildWorkflow`
- `crates/tokeira-kernel/src/kernel.rs` — populate new dispatch op fields
- `crates/tokeira-runtime/src/publisher.rs` — populate new `StartRequest` fields in `handle_start_child_workflow`
- `crates/tokeira-runtime/src/lane.rs` — populate new `StartRequest` fields in continue-as-new path
- `crates/tokeira-edge/src/grpc/translate.rs` — extract `retry_policy` for ContinueAsNew command, extract `control` for signal/cancel-external commands

### Component 6: History serializer — Populate proto fields

**Problem:** The history serializer's `WorkflowExecutionStarted` arm uses `..Default::default()` for parent fields, `original_execution_run_id`, `continued_failure`, and `last_completion_result`. The `WorkflowExecutionContinuedAsNew` arm ignores `workflow_execution_timeout` with `_` and doesn't populate `retry_policy`, `initiator`, `failure`, or `last_completion_result`. The signal-external and cancel-external initiated arms don't populate `control`.

**Design:**

**`WorkflowExecutionStarted` arm:**

```rust
HistoryEventKind::WorkflowExecutionStarted {
    // ... existing fields ...
    parent_workflow_id,
    parent_run_id,
    parent_namespace_id,
    parent_initiated_event_id,
    original_execution_run_id,
    continued_failure,
    last_completion_result,
} => Attributes::WorkflowExecutionStartedEventAttributes(
    history::WorkflowExecutionStartedEventAttributes {
        // ... existing field mappings ...
        parent_workflow_execution: parent_workflow_id.as_ref().map(|wid| {
            proto_common::WorkflowExecution {
                workflow_id: wid.0.clone(),
                run_id: parent_run_id.as_ref()
                    .map(|r| r.0.to_string())
                    .unwrap_or_default(),
            }
        }),
        parent_workflow_namespace_id: parent_namespace_id
            .as_ref()
            .map(|ns| ns.0.to_string())
            .unwrap_or_default(),
        parent_initiated_event_id: *parent_initiated_event_id,
        original_execution_run_id: opt_run_id(original_execution_run_id),
        continued_failure: continued_failure.as_ref().map(payload_to_failure),
        last_completion_result: last_completion_result.as_ref()
            .map(payloads_from_domain),
        ..Default::default()
    },
),
```

**`WorkflowExecutionContinuedAsNew` arm:**

```rust
HistoryEventKind::WorkflowExecutionContinuedAsNew {
    new_run_id,
    workflow_type,
    task_queue,
    input,
    memo,
    search_attributes,
    workflow_execution_timeout: _,  // proto doesn't have this field; kernel carries it for runtime use only
    workflow_run_timeout,
    workflow_task_timeout,
    retry_policy,
    initiator,
    failure,
    last_completion_result,
} => Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
    history::WorkflowExecutionContinuedAsNewEventAttributes {
        new_execution_run_id: new_run_id.0.to_string(),
        workflow_type: Some(proto_common::WorkflowType {
            name: workflow_type.0.clone(),
        }),
        task_queue: Some(task_queue_from_domain(task_queue)),
        input: Some(payloads_from_domain(input)),
        memo: Some(memo_from_domain(memo)),
        search_attributes: Some(search_attributes_from_domain(search_attributes)),
        workflow_run_timeout: to_opt_proto_duration(*workflow_run_timeout),
        workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
        // Previously ignored fields now populated:
        // Note: proto comment says workflow_execution_timeout is omitted,
        // but we populate it for completeness since the kernel carries it.
        retry_policy: retry_policy.as_ref().map(retry_policy_to_proto),
        initiator: continue_as_new_initiator_i32(initiator),
        failure: failure.as_ref().map(payload_to_failure),
        last_completion_result: last_completion_result.as_ref()
            .map(payloads_from_domain),
        ..Default::default()
    },
),
```

Note: The upstream proto comment says `workflow_execution_timeout` is "omitted as it shouldn't be overridden from within a workflow." The proto struct doesn't have a `workflow_execution_timeout` field. So we don't serialize it — the kernel carries it for internal use only. The `_` wildcard is correct for this field.

**Signal-external initiated arm:**

```rust
HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
    target_workflow_id,
    target_run_id,
    signal_name,
    input,
    control,  // NEW
} => Attributes::SignalExternalWorkflowExecutionInitiatedEventAttributes(
    history::SignalExternalWorkflowExecutionInitiatedEventAttributes {
        workflow_execution: Some(proto_common::WorkflowExecution {
            workflow_id: target_workflow_id.0.clone(),
            run_id: opt_run_id(target_run_id),
        }),
        signal_name: signal_name.clone(),
        input: Some(payloads_from_domain(input)),
        control: control.clone(),  // NEW
        ..Default::default()
    },
),
```

**Cancel-external initiated arm:**

```rust
HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
    target_workflow_id,
    target_run_id,
    control,  // NEW
} => Attributes::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(
    history::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
        workflow_execution: Some(proto_common::WorkflowExecution {
            workflow_id: target_workflow_id.0.clone(),
            run_id: opt_run_id(target_run_id),
        }),
        control: control.clone(),  // NEW
        ..Default::default()
    },
),
```

Add a helper for the initiator enum:

```rust
fn continue_as_new_initiator_i32(i: &ContinueAsNewInitiator) -> i32 {
    use tokeira_proto::enums::ContinueAsNewInitiator as P;
    (match i {
        ContinueAsNewInitiator::Workflow => P::Workflow,
        ContinueAsNewInitiator::Retry => P::Retry,
        ContinueAsNewInitiator::CronSchedule => P::CronSchedule,
    }) as i32
}
```

**Files changed:**
- `crates/tokeira-edge/src/translate/history_serializer.rs` — update `WorkflowExecutionStarted`, `WorkflowExecutionContinuedAsNew`, `SignalExternalWorkflowExecutionInitiated`, `RequestCancelExternalWorkflowExecutionInitiated` arms; add `continue_as_new_initiator_i32` helper

### Component 7: Fix downstream compilation and update tests

**Problem:** Adding fields to kernel event variants, command structs, and state structs will break pattern matches and construction sites across the codebase.

**Design:**

All pattern matches on the modified variants need updating:
- `kernel.rs` — `apply_start`, `apply_signal_with_start`, `apply_replayed_event`, `apply_workflow_task_completed`
- `history_serializer.rs` — already covered by Component 6
- `property_tests.rs` — proptest generators for events
- `golden_tests.rs` — golden test construction sites
- `grpc/translate.rs` — command translation
- `lane.rs` — continue-as-new event extraction
- `publisher.rs` — child workflow start

The proptest generators in `history_serializer.rs::tests` need to be updated to generate the new fields. The `arb_history_event_kind` function needs new arms for the enriched variants.

**Files changed:**
- `crates/tokeira-kernel/tests/property_tests.rs` — update generators and assertions
- `crates/tokeira-kernel/tests/golden_tests.rs` — update construction sites
- `crates/tokeira-edge/src/translate/history_serializer.rs` — update proptest generators in `mod tests`
- All other files with pattern matches on the modified variants

## Data Models

### Modified: `HistoryEventKind::WorkflowExecutionStarted` (kernel)

```rust
WorkflowExecutionStarted {
    workflow_type: WorkflowType,
    task_queue: TaskQueueName,
    input: Payloads,
    memo: Memo,
    search_attributes: SearchAttributes,
    request_id: String,
    continued_execution_run_id: Option<RunId>,
    first_execution_run_id: Option<RunId>,
    retry_policy: Option<RetryPolicy>,
    attempt: u32,
    workflow_execution_timeout: Option<Duration>,
    workflow_run_timeout: Option<Duration>,
    workflow_task_timeout: Duration,
    parent_workflow_id: Option<WorkflowId>,          // NEW
    parent_run_id: Option<RunId>,                    // NEW
    parent_namespace_id: Option<NamespaceId>,        // NEW
    parent_initiated_event_id: i64,                  // NEW (0 = no parent)
    original_execution_run_id: Option<RunId>,        // NEW
    continued_failure: Option<Payload>,              // NEW
    last_completion_result: Option<Payloads>,        // NEW
}
```

### Modified: `HistoryEventKind::WorkflowExecutionContinuedAsNew` (kernel)

```rust
WorkflowExecutionContinuedAsNew {
    new_run_id: RunId,
    workflow_type: WorkflowType,
    task_queue: TaskQueueName,
    input: Payloads,
    memo: Memo,
    search_attributes: SearchAttributes,
    workflow_execution_timeout: Option<Duration>,
    workflow_run_timeout: Option<Duration>,
    workflow_task_timeout: Duration,
    retry_policy: Option<RetryPolicy>,               // NEW
    initiator: ContinueAsNewInitiator,               // NEW
    failure: Option<Payload>,                        // NEW
    last_completion_result: Option<Payloads>,        // NEW
}
```

### Modified: `HistoryEventKind::SignalExternalWorkflowExecutionInitiated` (kernel)

```rust
SignalExternalWorkflowExecutionInitiated {
    target_workflow_id: WorkflowId,
    target_run_id: Option<RunId>,
    signal_name: String,
    input: Payloads,
    control: String,                                 // NEW
}
```

### Modified: `HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated` (kernel)

```rust
RequestCancelExternalWorkflowExecutionInitiated {
    target_workflow_id: WorkflowId,
    target_run_id: Option<RunId>,
    control: String,                                 // NEW
}
```

### New: `ContinueAsNewInitiator` (kernel)

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum ContinueAsNewInitiator {
    Workflow,
    Retry,
    CronSchedule,
}
```

### Modified: `StartRequest` (kernel)

```rust
pub struct StartRequest {
    // ... existing fields ...
    pub parent_run_id: Option<RunId>,                // NEW
    pub parent_namespace_id: Option<NamespaceId>,    // NEW
    pub parent_initiated_event_id: i64,              // NEW (0 = no parent)
    pub original_execution_run_id: Option<RunId>,    // NEW
    pub continued_failure: Option<Payload>,           // NEW
    pub last_completion_result: Option<Payloads>,    // NEW
}
```

### Modified: `SignalWithStartRequest` (kernel)

Same new fields as `StartRequest`.

### Modified: `WorkflowCommand::ContinueAsNew` (kernel)

```rust
ContinueAsNew {
    // ... existing fields ...
    retry_policy: Option<RetryPolicy>,               // NEW
}
```

### Modified: `WorkflowCommand::SignalExternalWorkflowExecution` (kernel)

```rust
SignalExternalWorkflowExecution {
    // ... existing fields ...
    control: String,                                 // NEW
}
```

### Modified: `WorkflowCommand::RequestCancelExternalWorkflowExecution` (kernel)

```rust
RequestCancelExternalWorkflowExecution {
    // ... existing fields ...
    control: String,                                 // NEW
}
```

### Modified: `WorkflowState` (kernel)

```rust
pub struct WorkflowState {
    // ... existing fields ...
    pub original_execution_run_id: Option<RunId>,    // NEW
    pub parent_run_id: Option<RunId>,                // NEW
    pub parent_namespace_id: Option<NamespaceId>,    // NEW
    pub parent_initiated_event_id: i64,              // NEW
    pub last_completion_result: Option<Payloads>,    // NEW
}
```

### Modified: `DispatchOp::StartChildWorkflow` (kernel)

```rust
StartChildWorkflow {
    // ... existing fields ...
    parent_run_id: RunId,                            // NEW
    parent_namespace_id: NamespaceId,                // NEW
}
```

## Correctness Properties

### Property 1: WorkflowExecutionStarted parent metadata round-trip

*For any* `HistoryEvent` with kind `WorkflowExecutionStarted` where `parent_workflow_id` is `Some(wid)`, `parent_run_id` is `Some(rid)`, `parent_namespace_id` is `Some(nsid)`, and `parent_initiated_event_id` is non-zero, serializing via `history_event_to_proto` SHALL produce a `WorkflowExecutionStartedEventAttributes` where `parent_workflow_execution` has `workflow_id == wid.0` and `run_id == rid.0.to_string()`, `parent_workflow_namespace_id == nsid.0.to_string()`, and `parent_initiated_event_id` equals the input value.

**Validates:** Requirements 1 (AC 1.1, 1.2, 1.3)

### Property 2: WorkflowExecutionStarted no-parent defaults

*For any* `HistoryEvent` with kind `WorkflowExecutionStarted` where `parent_workflow_id` is `None`, serializing via `history_event_to_proto` SHALL produce a `WorkflowExecutionStartedEventAttributes` where `parent_workflow_execution` is `None`, `parent_workflow_namespace_id` is empty, and `parent_initiated_event_id` is 0.

**Validates:** Requirement 1 (AC 1.4)

### Property 3: WorkflowExecutionStarted execution chain fields

*For any* `HistoryEvent` with kind `WorkflowExecutionStarted` where `original_execution_run_id` is `Some(rid)`, serializing via `history_event_to_proto` SHALL produce a `WorkflowExecutionStartedEventAttributes` where `original_execution_run_id == rid.0.to_string()`.

**Validates:** Requirement 2 (AC 2.1)

### Property 4: WorkflowExecutionStarted continued_failure round-trip

*For any* `HistoryEvent` with kind `WorkflowExecutionStarted` where `continued_failure` is `Some(payload)` containing a proto `Failure`, serializing via `history_event_to_proto` SHALL produce a `WorkflowExecutionStartedEventAttributes` where `continued_failure` is `Some` and contains a proto `Failure` with the original message preserved.

**Validates:** Requirement 3 (AC 3.1, 3.2)

### Property 5: WorkflowExecutionStarted last_completion_result round-trip

*For any* `HistoryEvent` with kind `WorkflowExecutionStarted` where `last_completion_result` is `Some(payloads)`, serializing via `history_event_to_proto` SHALL produce a `WorkflowExecutionStartedEventAttributes` where `last_completion_result` is `Some`.

**Validates:** Requirement 4 (AC 4.1, 4.2)

### Property 6: WorkflowExecutionContinuedAsNew enriched fields

*For any* `HistoryEvent` with kind `WorkflowExecutionContinuedAsNew` where `retry_policy` is `Some`, `initiator` is any variant, `failure` is `Some(payload)`, and `last_completion_result` is `Some(payloads)`, serializing via `history_event_to_proto` SHALL produce a `WorkflowExecutionContinuedAsNewEventAttributes` where `retry_policy` is `Some`, `initiator` is non-zero, `failure` is `Some`, and `last_completion_result` is `Some`.

**Validates:** Requirements 6, 7, 8 (AC 6.1, 7.1, 8.3, 8.4)

### Property 7: Signal-external and cancel-external control field

*For any* `HistoryEvent` with kind `SignalExternalWorkflowExecutionInitiated` where `control` is a non-empty string, serializing via `history_event_to_proto` SHALL produce a `SignalExternalWorkflowExecutionInitiatedEventAttributes` where `control` equals the input string. The same property holds for `RequestCancelExternalWorkflowExecutionInitiated`.

**Validates:** Requirements 10, 11 (AC 10.1, 11.1)

### Property 8: ActivityTaskScheduled timeout completeness

*For any* `HistoryEvent` with kind `ActivityTaskScheduled` where `schedule_to_close_timeout` is `Some(d)`, serializing via `history_event_to_proto` SHALL produce an `ActivityTaskScheduledEventAttributes` where `schedule_to_close_timeout` is `Some` with positive duration. The same property holds for `schedule_to_start_timeout`, `start_to_close_timeout`, and `heartbeat_timeout`.

**Validates:** Requirement 9 (AC 9.1–9.5)

## Error Handling

No new error paths are introduced. All changes add data to existing success paths:

- Parent metadata fields default to `None`/0 when not a child workflow — the serializer produces empty/default proto values, which is correct.
- `original_execution_run_id` defaults to `Some(run_id)` for the first run — the kernel handles this in `apply_start`.
- `continued_failure` and `last_completion_result` default to `None` — the serializer produces absent proto fields, which is correct.
- `retry_policy` on `ContinuedAsNew` defaults to `None` — the serializer produces an absent proto field.
- `initiator` defaults to `ContinueAsNewInitiator::Workflow` — the serializer produces the correct proto enum value.
- `control` defaults to empty string — the serializer produces an empty proto string, which is the correct default.
- If `payload_to_failure` receives a corrupted `Payload` for `continued_failure`, it falls back to interpreting the bytes as a UTF-8 message string (existing behavior from Feature 2).

## Testing Strategy

### Property-based tests (proptest, 100 iterations)

1. **WorkflowExecutionStarted parent metadata** — Generate arbitrary `HistoryEvent` values with `WorkflowExecutionStarted` kind carrying random parent_workflow_id, parent_run_id, parent_namespace_id, and parent_initiated_event_id. Serialize to proto and assert the parent fields are correctly mapped. Also test with `None` parent fields and assert defaults. (Properties 1, 2)

2. **WorkflowExecutionStarted execution chain fields** — Generate arbitrary events with random `original_execution_run_id`, `continued_failure`, and `last_completion_result`. Serialize and assert the proto fields match. (Properties 3, 4, 5)

3. **WorkflowExecutionContinuedAsNew enriched fields** — Generate arbitrary events with random `retry_policy`, `initiator`, `failure`, and `last_completion_result`. Serialize and assert the proto fields are populated. (Property 6)

4. **Signal/cancel-external control field** — Generate arbitrary events with random `control` strings. Serialize and assert the proto `control` field matches. (Property 7)

5. **ActivityTaskScheduled timeout completeness** — Already covered by the existing `prop_history_serialization_round_trip` test. Verify that the existing test covers all four timeout fields. (Property 8)

### Unit tests (example-based)

- History serializer: `WorkflowExecutionStarted` with parent metadata produces proto with `parent_workflow_execution` populated
- History serializer: `WorkflowExecutionStarted` without parent produces proto with empty parent fields
- History serializer: `WorkflowExecutionStarted` with `original_execution_run_id` produces proto with the field populated
- History serializer: `WorkflowExecutionStarted` with `continued_failure` produces proto with `continued_failure` populated
- History serializer: `WorkflowExecutionContinuedAsNew` with `retry_policy` produces proto with `retry_policy` populated
- History serializer: `WorkflowExecutionContinuedAsNew` with `initiator: Workflow` produces proto with `initiator = 1`
- History serializer: `SignalExternalWorkflowExecutionInitiated` with `control` produces proto with `control` populated
- Kernel: `apply_start` with parent fields produces `WorkflowExecutionStarted` event with parent fields
- Kernel: `apply_start` without `original_execution_run_id` sets it to `run_id`
- Kernel: `ContinueAsNew` command produces event with `initiator: Workflow` and `retry_policy` from state