# Requirements Document: External Signal and Cancel Delivery

## Introduction

This document captures the requirements for Feature 7 of the Tokeira runtime: External Signal and Cancel Delivery. This feature wires the runtime's `DispatchPublisher` to handle the two external workflow dispatch ops (`SignalExternalWorkflow`, `RequestCancelExternalWorkflow`) and delivers resolution results back to the originating workflow.

External signal and cancel delivery is structurally similar to child workflow orchestration (Feature 6). When a workflow issues a `SignalExternalWorkflowExecution` or `RequestCancelExternalWorkflowExecution` command via a workflow task completion, the kernel emits a `DispatchOp::SignalExternalWorkflow` or `DispatchOp::RequestCancelExternalWorkflow`. The runtime must then:

1. Resolve the target workflow execution to a `RunKey` via `resolve_execution`.
2. Submit a `Command::Signal` or `Command::Cancel` to the target run.
3. Deliver a resolution command (`Command::ExternalSignalResolved` or `Command::ExternalCancelResolved`) back to the originating run with either a success or failure result.

The kernel already handles all external signal/cancel commands authoritatively:
- `WorkflowCommand::SignalExternalWorkflowExecution` emits a `DispatchOp::SignalExternalWorkflow` and inserts a `PendingExternalSignal` entry in the originator's `pending_external_signals` map.
- `WorkflowCommand::RequestCancelExternalWorkflowExecution` emits a `DispatchOp::RequestCancelExternalWorkflow` and inserts a `PendingExternalCancel` entry in the originator's `pending_external_cancels` map.
- `Command::ExternalSignalResolved` emits the appropriate history event and removes the entry from `pending_external_signals`.
- `Command::ExternalCancelResolved` emits the appropriate history event and removes the entry from `pending_external_cancels`.

The `RuntimeDispatchPublisher` currently logs external signal/cancel dispatch ops as stubs (falling through to the `other =>` catch-all arm). This feature replaces those stubs with working implementations.

A key design consideration is originator identity propagation. The current `DispatchOp::SignalExternalWorkflow` and `DispatchOp::RequestCancelExternalWorkflow` do not carry the originator's `RunKey` or `initiated_event_id`. The publisher needs both to deliver the resolution command back to the originator. This is the same pattern solved in Feature 6 for `StartChildWorkflow`, which was extended with `parent_run_key`, `parent_workflow_id`, and `initiated_event_id`. The dispatch ops must be similarly extended.

Unlike child workflow orchestration, external signals and cancels target arbitrary workflows (not just children) and may target workflows in different namespaces. The dispatch ops carry `target_workflow_id` and an optional `target_run_id` rather than a `child_run_id`.

The publisher already has repository access (added in Feature 6) for `resolve_execution`, and `submit_to_run` for delivering resolution commands back to the originator.

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing) and Feature 6 (Child Workflow Orchestration), both already implemented. Feature 6 added repository access and `submit_to_run` to the `RuntimeDispatchPublisher`, which this feature reuses.

The authoritative specifications are [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md) and [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md).

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one lane via `hash(run_key) mod lane_count`.
- **DispatchPublisher**: The trait responsible for publishing dispatch ops from committed transitions. The `RuntimeDispatchPublisher` implements this trait and routes ops to the appropriate subsystem.
- **DispatchOp_SignalExternalWorkflow**: A dispatch op emitted by the kernel when a workflow task completion includes a `WorkflowCommand::SignalExternalWorkflowExecution`. Carries `target_workflow_id`, optional `target_run_id`, `signal_name`, and `input`.
- **DispatchOp_RequestCancelExternalWorkflow**: A dispatch op emitted by the kernel when a workflow task completion includes a `WorkflowCommand::RequestCancelExternalWorkflowExecution`. Carries `target_workflow_id` and optional `target_run_id`.
- **ExternalSignalResolved**: A kernel command (`Command::ExternalSignalResolved`) delivered to the originating run after the runtime attempts to deliver the signal. Contains `ExternalSignalResult::Signaled` or `ExternalSignalResult::Failed { cause }`.
- **ExternalCancelResolved**: A kernel command (`Command::ExternalCancelResolved`) delivered to the originating run after the runtime attempts to deliver the cancel. Contains `ExternalCancelResult::CancelRequested` or `ExternalCancelResult::Failed { cause }`.
- **PendingExternalSignal**: Kernel state tracking an in-flight signal to an external workflow in the originator's `WorkflowState.pending_external_signals` map. Keyed by `initiated_event_id`.
- **PendingExternalCancel**: Kernel state tracking an in-flight cancel request to an external workflow in the originator's `WorkflowState.pending_external_cancels` map. Keyed by `initiated_event_id`.
- **Originator_Run_Key**: The `RunKey` of the workflow run that initiated the external signal or cancel. Required by the runtime to deliver `ExternalSignalResolved` or `ExternalCancelResolved` commands back to the originator.
- **Target_Workflow**: The workflow execution that is the recipient of the signal or cancel request. Identified by `target_workflow_id` and optionally `target_run_id`.
- **ExecutionRef**: Composite reference (`namespace_id`, `workflow_id`, optional `run_id`) used by `resolve_execution` to map a workflow identity to a `RunKey`.

## Requirements

---

### Requirement 1: Signal External Workflow Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle `DispatchOp::SignalExternalWorkflow`, so that workflows can signal other workflow executions.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::SignalExternalWorkflow`, THE Runtime SHALL resolve the target workflow execution using `resolve_execution` with the `target_workflow_id` and optional `target_run_id` from the dispatch op.
2. WHEN the target execution resolves to a `RunKey`, THE Runtime SHALL submit a `Command::Signal` to the target run with the `signal_name` and `input` from the dispatch op.
3. WHEN the signal delivery succeeds (the `Command::Signal` is committed), THE Runtime SHALL submit a `Command::ExternalSignalResolved` to the originating run with `ExternalSignalResult::Signaled` and the `initiated_event_id` from the dispatch op.
4. WHEN the signal delivery fails because the target is not found (`resolve_execution` returns `None`), THE Runtime SHALL submit a `Command::ExternalSignalResolved` to the originating run with `ExternalSignalResult::Failed { cause }` describing the failure.
5. WHEN the signal delivery fails because the target run is closed (kernel rejects with `RunClosed`), THE Runtime SHALL submit a `Command::ExternalSignalResolved` to the originating run with `ExternalSignalResult::Failed { cause }` describing the failure.
6. WHEN the signal delivery fails due to a transient error (storage unavailable, lane channel closed), THE Runtime SHALL submit a `Command::ExternalSignalResolved` to the originating run with `ExternalSignalResult::Failed { cause }` describing the failure.

---

### Requirement 2: Cancel External Workflow Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle `DispatchOp::RequestCancelExternalWorkflow`, so that workflows can request cancellation of other workflow executions.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::RequestCancelExternalWorkflow`, THE Runtime SHALL resolve the target workflow execution using `resolve_execution` with the `target_workflow_id` and optional `target_run_id` from the dispatch op.
2. WHEN the target execution resolves to a `RunKey`, THE Runtime SHALL submit a `Command::Cancel` to the target run.
3. WHEN the cancel delivery succeeds (the `Command::Cancel` is committed), THE Runtime SHALL submit a `Command::ExternalCancelResolved` to the originating run with `ExternalCancelResult::CancelRequested` and the `initiated_event_id` from the dispatch op.
4. WHEN the cancel delivery fails because the target is not found (`resolve_execution` returns `None`), THE Runtime SHALL submit a `Command::ExternalCancelResolved` to the originating run with `ExternalCancelResult::Failed { cause }` describing the failure.
5. WHEN the cancel delivery fails because the target run is closed (kernel rejects with `RunClosed`), THE Runtime SHALL submit a `Command::ExternalCancelResolved` to the originating run with `ExternalCancelResult::Failed { cause }` describing the failure.
6. WHEN the cancel delivery fails due to a transient error (storage unavailable, lane channel closed), THE Runtime SHALL submit a `Command::ExternalCancelResolved` to the originating run with `ExternalCancelResult::Failed { cause }` describing the failure.

---

### Requirement 3: Cross-Namespace Signal and Cancel Routing

**User Story:** As a Tokeira developer, I want external signal and cancel dispatch to support cross-namespace routing, so that workflows in different namespaces can communicate via signal and cancel.

#### Acceptance Criteria

1. THE `WorkflowCommand::SignalExternalWorkflowExecution` variant SHALL be extended with a `target_namespace_id: NamespaceId` field identifying the namespace of the target workflow.
2. THE `WorkflowCommand::RequestCancelExternalWorkflowExecution` variant SHALL be extended with a `target_namespace_id: NamespaceId` field identifying the namespace of the target workflow.
3. THE kernel's `apply_workflow_command` SHALL propagate `target_namespace_id` from the workflow command into the `DispatchOp::SignalExternalWorkflow` and `DispatchOp::RequestCancelExternalWorkflow` dispatch ops as the `namespace_id` field used for target resolution.
4. WHEN a `DispatchOp::SignalExternalWorkflow` or `DispatchOp::RequestCancelExternalWorkflow` targets a workflow in a different namespace, THE Runtime SHALL resolve the target execution in the target namespace by passing the dispatch op's `namespace_id` to `resolve_execution`.
5. THE Runtime SHALL use the same `resolve_execution` mechanism regardless of whether the target is in the same or a different namespace.

---

### Requirement 4: Originator Identity Propagation via Dispatch Op Extension

**User Story:** As a Tokeira developer, I want the dispatch ops for external signal and cancel to carry the originator's identity, so that the publisher can deliver resolution results back to the originating run.

#### Acceptance Criteria

1. THE `DispatchOp::SignalExternalWorkflow` variant SHALL carry `originator_run_key: RunKey` identifying the run that initiated the signal.
2. THE `DispatchOp::SignalExternalWorkflow` variant SHALL carry `namespace_id: NamespaceId` identifying the namespace of the target workflow for execution resolution.
3. THE `DispatchOp::SignalExternalWorkflow` variant SHALL carry `initiated_event_id: i64` matching the event ID of the `SignalExternalWorkflowExecutionInitiated` history event, so the resolution command can reference the correct pending entry.
4. THE `DispatchOp::RequestCancelExternalWorkflow` variant SHALL carry `originator_run_key: RunKey` identifying the run that initiated the cancel.
5. THE `DispatchOp::RequestCancelExternalWorkflow` variant SHALL carry `namespace_id: NamespaceId` identifying the namespace of the target workflow for execution resolution.
6. THE `DispatchOp::RequestCancelExternalWorkflow` variant SHALL carry `initiated_event_id: i64` matching the event ID of the `RequestCancelExternalWorkflowExecutionInitiated` history event, so the resolution command can reference the correct pending entry.
7. THE kernel's `apply_workflow_command` for `SignalExternalWorkflowExecution` SHALL populate `originator_run_key` from `builder.state.run_key`, `namespace_id` from the workflow command's `target_namespace_id`, and `initiated_event_id` from the emitted event ID.
8. THE kernel's `apply_workflow_command` for `RequestCancelExternalWorkflowExecution` SHALL populate `originator_run_key` from `builder.state.run_key`, `namespace_id` from the workflow command's `target_namespace_id`, and `initiated_event_id` from the emitted event ID.

---

### Requirement 5: Resolution Delivery Always Reaches Originator

**User Story:** As a Tokeira developer, I want the runtime to always deliver a resolution result to the originating run after attempting an external signal or cancel, so that the originator is never left waiting indefinitely for a resolution that will not arrive.

#### Acceptance Criteria

1. THE Runtime SHALL deliver a `Command::ExternalSignalResolved` to the originating run regardless of whether the signal delivery succeeded or failed.
2. THE Runtime SHALL deliver a `Command::ExternalCancelResolved` to the originating run regardless of whether the cancel delivery succeeded or failed.
3. IF the resolution delivery to the originator fails (originator lane closed, OCC exhaustion), THEN THE Runtime SHALL log the failure at warn level. The originator's `PendingExternalSignal` or `PendingExternalCancel` entry will remain until the sweeper (Feature 11) or a future reconciliation mechanism resolves it.

---

### Requirement 6: Dispatch Error Resilience

**User Story:** As a Tokeira developer, I want external signal and cancel dispatch operations to be resilient to transient errors, so that temporary failures do not leave the originator in an inconsistent state.

#### Acceptance Criteria

1. IF `resolve_execution` encounters a transient error (storage unavailable), THEN THE Runtime SHALL deliver a `Command::ExternalSignalResolved` or `Command::ExternalCancelResolved` with a `Failed` result to the originating run.
2. IF the `Command::Signal` or `Command::Cancel` submission to the target run encounters a transient error (lane channel closed, OCC exhaustion), THEN THE Runtime SHALL deliver a resolution with a `Failed` result to the originating run.
3. THE Runtime SHALL process external signal and cancel dispatch ops asynchronously (via `tokio::spawn`); failure in one dispatch SHALL NOT block processing of other dispatch ops in the same batch.
4. IF the resolution delivery to the originator fails, THEN THE Runtime SHALL log the failure at warn level and continue processing remaining dispatch ops.

---

### Requirement 7: Signal Command Construction

**User Story:** As a Tokeira developer, I want the `Command::Signal` submitted to the target run to carry the correct signal name and input, so that the target workflow receives the intended signal.

#### Acceptance Criteria

1. THE `Command::Signal` submitted to the target run SHALL carry the `signal_name` from the dispatch op.
2. THE `Command::Signal` submitted to the target run SHALL carry the `input` payload from the dispatch op.
3. THE `Command::Signal` submitted to the target run SHALL carry a `RequestContext` with a unique `request_id` and a `caller_identity` identifying the runtime's external signal orchestrator.

---

### Requirement 8: Cancel Command Construction

**User Story:** As a Tokeira developer, I want the `Command::Cancel` submitted to the target run to carry appropriate metadata, so that the target workflow receives a well-formed cancel request.

#### Acceptance Criteria

1. THE `Command::Cancel` submitted to the target run SHALL carry a `reason` describing that the cancel was initiated by an external workflow.
2. THE `Command::Cancel` submitted to the target run SHALL carry a `RequestContext` with a unique `request_id` and a `caller_identity` identifying the runtime's external cancel orchestrator.
3. THE `Command::Cancel` submitted to the target run SHALL carry an `external_initiator` field populated with the originating workflow's namespace, workflow ID, and run ID, so the target's history records the source of the cancel request.
