# Requirements Document: Continue-As-New

## Introduction

This document captures the requirements for Feature 8 (Continue-As-New) of the Tokeira runtime. Continue-as-new is the mechanism by which a workflow closes its current run and starts a successor run with fresh history, preserving logical execution identity across the chain.

The kernel already handles the authoritative close: `WorkflowCommand::ContinueAsNew` emits a `WorkflowExecutionContinuedAsNew` history event carrying the successor's `new_run_id`, `workflow_type`, `task_queue`, `input`, `memo`, `search_attributes`, and timeout configuration, then closes the run with `ExecutionStatus::ContinuedAsNew`. The runtime's job is to detect this close in the lane's post-commit path, extract the event parameters, and issue a `Command::Start` for the successor run.

This feature also completes the chain-aware execution timeout story deferred by Feature 5 (Workflow Timeouts). The execution timeout should measure wall-clock time from the first run in the chain, not from the current run's `started_at`. This requires propagating `first_run_started_at` through the `StartRequest` and `WorkflowState` so the workflow timeout scanner can use the chain origin timestamp.

This feature depends on Feature 1 (Lane OCC Retry), Feature 5 (Workflow Timeouts), and Feature 6 (Child Workflow Orchestration). The lane post-commit detection pattern established by Feature 6 (child resolution) is reused here for continue-as-new successor creation.

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one lane via `hash(run_key) mod lane_count`.
- **Kernel**: The pure state-transition engine (`tokeira-kernel`) that computes authoritative transitions from loaded state and commands.
- **Successor_Run**: The new workflow run created when the predecessor closes with `ExecutionStatus::ContinuedAsNew`. The successor has a fresh `RunKey`, `RunId`, and empty history.
- **Predecessor_Run**: The workflow run that closed with `ExecutionStatus::ContinuedAsNew`, triggering successor creation.
- **Execution_Chain**: The sequence of runs linked by continue-as-new, sharing the same `workflow_id` and connected via `continued_execution_run_id` and `first_execution_run_id`.
- **ContinuedAsNew_Event**: The `HistoryEventKind::WorkflowExecutionContinuedAsNew` history event emitted by the kernel, carrying successor parameters (`new_run_id`, `workflow_type`, `task_queue`, `input`, `memo`, `search_attributes`, timeout config).
- **DispatchPublisher**: The trait used by the lane to publish dispatch ops and submit commands to other runs after a committed transition.
- **WorkflowTimeoutTrackingState**: Runtime-local in-memory state tracking open runs with timeout configuration, used by the workflow timeout scanner.
- **StartRequest**: The command payload for creating a new workflow execution, carrying all parameters including chain identity fields (`continued_execution_run_id`, `first_execution_run_id`).

## Requirements

---

### Requirement 1: Successor Run Detection

**User Story:** As a Tokeira developer, I want the runtime to detect when a run closes with ContinuedAsNew status, so that the successor run creation process is triggered.

#### Acceptance Criteria

1. WHEN a committed transition closes a run with `ExecutionStatus::ContinuedAsNew`, THE Lane SHALL identify the `WorkflowExecutionContinuedAsNew` event from the committed transition's history events.
2. WHEN a committed transition closes a run with a status other than `ContinuedAsNew`, THE Lane SHALL NOT attempt successor run creation.
3. IF the committed transition has `ExecutionStatus::ContinuedAsNew` but no `WorkflowExecutionContinuedAsNew` event is found in the history events, THEN THE Lane SHALL log the anomaly at error level and skip successor creation.

---

### Requirement 2: Successor StartRequest Construction

**User Story:** As a Tokeira developer, I want the runtime to construct a correct StartRequest for the successor run from the ContinuedAsNew event, so that the successor inherits the intended parameters.

#### Acceptance Criteria

1. THE Runtime SHALL construct a `StartRequest` for the successor run using the `new_run_id` from the ContinuedAsNew_Event as the successor's `run_id`.
2. THE Runtime SHALL set the successor's `workflow_type`, `task_queue`, `input`, `memo`, `search_attributes`, `workflow_execution_timeout`, `workflow_run_timeout`, and `workflow_task_timeout` from the corresponding fields of the ContinuedAsNew_Event.
3. THE Runtime SHALL generate a fresh `RunKey` for the successor run.
4. THE Runtime SHALL set the successor's `workflow_id` to the predecessor's `workflow_id`.
5. THE Runtime SHALL set the successor's `namespace_id` to the predecessor's `namespace_id`.
6. THE Runtime SHALL set the successor's `continued_execution_run_id` to the predecessor's `run_id`.
7. THE Runtime SHALL set the successor's `attempt` to 1.
8. THE Runtime SHALL set the successor's `retry_policy` to the predecessor's `retry_policy`.
9. THE Runtime SHALL set the successor's `parent_run_key` and `parent_workflow_id` to `None` (the successor is not a child workflow; the predecessor's parent relationship does not transfer).

---

### Requirement 3: Execution Chain Identity Propagation

**User Story:** As a Tokeira developer, I want continue-as-new to preserve execution chain identity, so that the logical workflow execution can be traced across all runs in the chain.

#### Acceptance Criteria

1. WHEN the predecessor run has a `first_execution_run_id` value in its `WorkflowState`, THE Runtime SHALL set the successor's `first_execution_run_id` to that same value.
2. WHEN the predecessor run does not have a `first_execution_run_id` value (the predecessor is the first run in the chain), THE Runtime SHALL set the successor's `first_execution_run_id` to the predecessor's `run_id`.
3. THE `first_execution_run_id` on the successor's `StartRequest` SHALL reference the `run_id` of the very first run in the execution chain, regardless of chain length.

---

### Requirement 4: Chain-Aware Execution Timeout Propagation

**User Story:** As a Tokeira developer, I want the successor run to carry the chain origin timestamp, so that the workflow timeout scanner can measure execution timeout from the first run in the chain rather than from the current run's start time.

#### Acceptance Criteria

1. THE `StartRequest` SHALL carry a `first_run_started_at: Option<OffsetDateTime>` field representing the `started_at` timestamp of the first run in the execution chain.
2. THE `WorkflowState` SHALL carry a `first_run_started_at: Option<OffsetDateTime>` field, populated from the `StartRequest` during `apply_start`.
3. WHEN the predecessor run has a `first_run_started_at` value, THE Runtime SHALL propagate it to the successor's `StartRequest`.
4. WHEN the predecessor run does not have a `first_run_started_at` value (the predecessor is the first run in the chain), THE Runtime SHALL set the successor's `first_run_started_at` to the predecessor's `started_at`.
5. WHEN a run has a `first_run_started_at` value and a configured `workflow_execution_timeout`, THE WorkflowTimeoutTrackingState entry SHALL use `first_run_started_at` instead of `started_at` for execution timeout measurement.
6. WHEN a run does not have a `first_run_started_at` value, THE WorkflowTimeoutTrackingState entry SHALL use `started_at` for execution timeout measurement (backward-compatible with non-chain runs).

---

### Requirement 5: Successor Start Execution

**User Story:** As a Tokeira developer, I want the runtime to issue the successor Start command through the lane, so that the successor run is created authoritatively.

#### Acceptance Criteria

1. THE Runtime SHALL submit the successor's `Command::Start` to the lane determined by the successor's `RunKey`.
2. WHEN the successor `Command::Start` succeeds (returns `CommitResult::Applied`), THE Runtime SHALL consider the continue-as-new chain link complete.
3. THE Runtime SHALL execute successor start asynchronously after the predecessor's transition is committed; successor start failure SHALL NOT affect the predecessor's committed close.

---

### Requirement 6: Successor Start Failure Handling

**User Story:** As a Tokeira developer, I want the runtime to handle successor start failures gracefully, so that continue-as-new failures do not leave the execution chain in an inconsistent state.

#### Acceptance Criteria

1. IF the successor `Command::Start` fails, THEN THE Runtime SHALL log the failure at error level with the predecessor's `run_key`, `workflow_id`, `run_id`, the successor's `new_run_id`, and the error details.
2. IF the successor `Command::Start` fails, THEN THE Runtime SHALL NOT attempt to reopen or modify the already-closed predecessor run.
3. THE Runtime SHALL retry the successor `Command::Start` with bounded retries (reusing the lane's OCC retry mechanism) before giving up.
4. IF the successor `Command::Start` returns `CommitResult::Duplicate` (request-dedupe collision), THEN THE Runtime SHALL treat it as a failure and log at error level. Note: `Duplicate` from the current storage contract indicates a request-dedupe collision, not "successor already exists." A retried continue-as-new start uses a fresh `RunKey` and would not normally hit this path.

---

### Requirement 7: Successor Workflow Timeout Tracking

**User Story:** As a Tokeira developer, I want the successor run's timeout configuration to be tracked by the workflow timeout scanner, so that execution and run timeouts are enforced on the successor.

#### Acceptance Criteria

1. WHEN the successor `Command::Start` is committed and the successor has a non-None `workflow_execution_timeout` or `workflow_run_timeout`, THE Runtime SHALL insert a `WorkflowTimeoutEntry` into the `WorkflowTimeoutTrackingState` for the successor.
2. THE `WorkflowTimeoutEntry` for the successor SHALL use the committed successor's `new_state.started_at` for run timeout measurement, and `first_run_started_at` (if present) for execution timeout measurement.
3. WHEN the predecessor run closes with `ContinuedAsNew`, THE Lane SHALL remove the predecessor's `WorkflowTimeoutEntry` from the `WorkflowTimeoutTrackingState` (this is already handled by the existing post-commit `closed_at` check).
