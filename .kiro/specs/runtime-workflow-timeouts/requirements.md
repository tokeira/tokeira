# Requirements Document: Workflow Timeouts

## Introduction

This document captures the requirements for Feature 5 of the Tokeira runtime: Workflow Timeouts. This feature adds a background scanner that detects when open workflow runs exceed their configured execution or run timeouts and injects `Command::WorkflowExecutionTimedOut` commands into the owning run's lane mailbox.

Workflow timeout detection is non-authoritative. The scanner is a delivery mechanism, not a state authority. The authoritative state transition happens when the kernel processes the `WorkflowExecutionTimedOut` command — it emits a `WorkflowExecutionTimedOut` history event, closes the run with `ExecutionStatus::TimedOut`, deletes all open activities and timers, and applies parent close policy. If the scanner fires a stale or duplicate timeout (run already closed, run absent), the kernel rejects it harmlessly via `Reject::RunClosed` or `Reject::MissingRun`.

Unlike the timer scanner (Feature 4), which reads from a dedicated storage timer bucket, the workflow timeout scanner needs to check open runs' `WorkflowState` for timeout configuration (`workflow_execution_timeout`, `workflow_run_timeout`) and the `started_at` timestamp. The chosen approach is runtime-local tracking state — consistent with the activity timeout scanner (Feature 3). The runtime tracks open runs that have timeout configuration and checks them periodically. This avoids expensive full-table scans of all open runs in storage.

There are two distinct timeout types:
- **Execution timeout** (`workflow_execution_timeout`): bounds the wall-clock time for the current run. Measured from `started_at` of the current run. Note: Temporal's execution timeout semantically spans the entire execution chain (including continue-as-new and retry runs). Chain-aware measurement requires a `first_run_started_at` timestamp that is not yet carried in `StartRequest` or `WorkflowState`. This is deferred to Feature 8 (Continue-As-New), which handles chain identity propagation. For now, execution timeout is measured from the current run's `started_at`, which is correct for single-run workflows and a conservative approximation for chains.
- **Run timeout** (`workflow_run_timeout`): bounds the wall-clock time for a single run. Measured from `started_at` of the current run.

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing), which is already implemented.

The authoritative specifications are [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md) and [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md).

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one lane via `hash(run_key) mod lane_count`.
- **Workflow_Timeout_Scanner**: A background task in the Runtime that periodically checks tracked open runs for workflow execution timeout and workflow run timeout violations, and submits `Command::WorkflowExecutionTimedOut` commands through the lane when violations are detected.
- **Workflow_Execution_Timeout**: The maximum wall-clock time for the current run. Configured per workflow via `WorkflowState.workflow_execution_timeout`. Measured from the `started_at` of the current run. Note: chain-aware measurement across continue-as-new/retry is deferred to Feature 8.
- **Workflow_Run_Timeout**: The maximum wall-clock time for a single run. Configured per workflow via `WorkflowState.workflow_run_timeout`. Measured from the `started_at` of the current run.
- **WorkflowTimeoutType**: Kernel enum distinguishing `ExecutionTimeout` from `RunTimeout`, carried in the `WorkflowExecutionTimedOutRequest`.
- **RetryState**: Kernel enum describing the retry decision outcome, carried in the `WorkflowExecutionTimedOutRequest`.
- **Workflow_Timeout_Tracking_State**: Runtime-local state that tracks open runs with timeout configuration. Keyed by `RunKey`, stores the timeout durations and the relevant `started_at` timestamp needed for timeout detection. This state is not part of the kernel's `WorkflowState` — it is a runtime-side tracking structure populated by lifecycle hooks.
- **CancellationToken**: A cooperative shutdown signal (`tokio_util::sync::CancellationToken`) used to gracefully stop the Workflow_Timeout_Scanner background task when the Runtime shuts down.

## Requirements

---

### Requirement 1: Workflow Execution Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect workflow execution timeouts, so that workflows that exceed their configured execution timeout are terminated.

#### Acceptance Criteria

1. WHEN a workflow has a configured `workflow_execution_timeout` and the elapsed time since the workflow's `started_at` exceeds the timeout, THE Workflow_Timeout_Scanner SHALL submit a `Command::WorkflowExecutionTimedOut` with `timeout_type` set to `WorkflowTimeoutType::ExecutionTimeout` to the owning run via the lane.
2. THE Workflow_Timeout_Scanner SHALL set the `retry_state` field in the `WorkflowExecutionTimedOutRequest` to `RetryState::Timeout` when the workflow has a retry policy configured, or `RetryState::RetryPolicyNotSet` when no retry policy is configured. This feature does not evaluate retry eligibility (max attempts, non-retryable errors); full retry evaluation is a runtime concern handled by the retry/continue-as-new path.
3. THE Workflow_Timeout_Scanner SHALL set the `now` field in the `WorkflowExecutionTimedOutRequest` to the wall-clock time at which the timeout violation was detected.
4. THE Workflow_Timeout_Scanner SHALL only check execution timeout for runs that have a non-None `workflow_execution_timeout` in the Workflow_Timeout_Tracking_State.

---

### Requirement 2: Workflow Run Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect workflow run timeouts, so that individual runs within a retry or continue-as-new chain are bounded.

#### Acceptance Criteria

1. WHEN a workflow has a configured `workflow_run_timeout` and the elapsed time since the current run's `started_at` exceeds the timeout, THE Workflow_Timeout_Scanner SHALL submit a `Command::WorkflowExecutionTimedOut` with `timeout_type` set to `WorkflowTimeoutType::RunTimeout` to the owning run via the lane.
2. THE Workflow_Timeout_Scanner SHALL check workflow run timeouts as part of the same scan cycle used for execution timeouts.
3. WHEN both execution timeout and run timeout fire for the same run in the same scan cycle, THE Workflow_Timeout_Scanner SHALL submit only one `WorkflowExecutionTimedOut` command. The execution timeout SHALL take precedence over the run timeout.
4. THE Workflow_Timeout_Scanner SHALL only check run timeout for runs that have a non-None `workflow_run_timeout` in the Workflow_Timeout_Tracking_State.

---

### Requirement 3: Workflow Timeout Is Non-Authoritative

**User Story:** As a Tokeira developer, I want workflow timeout detection to be non-authoritative, so that duplicate or stale timeout commands are harmless and the kernel remains the single source of truth.

#### Acceptance Criteria

1. THE Workflow_Timeout_Scanner SHALL NOT modify authoritative state directly; the authoritative transition happens when the Kernel processes the `WorkflowExecutionTimedOut` command and commits the resulting `WorkflowExecutionTimedOut` history event and run closure.
2. WHEN a `WorkflowExecutionTimedOut` command is delivered for a run that is already closed, THE Kernel SHALL reject it with `Reject::RunClosed`, and THE Runtime SHALL treat that rejection as a harmless no-op.
3. WHEN a `WorkflowExecutionTimedOut` command is delivered for a run that does not exist, THE Kernel SHALL reject it with `Reject::MissingRun`, and THE Runtime SHALL treat that rejection as a harmless no-op.

---

### Requirement 4: Workflow Timeout Tracking State Management

**User Story:** As a Tokeira developer, I want the runtime to track open runs with timeout configuration, so that the timeout scanner can check them without scanning all open runs in storage.

#### Acceptance Criteria

1. WHEN a workflow is started via `Command::Start` and the `StartRequest` contains a non-None `workflow_execution_timeout` or `workflow_run_timeout`, THE Runtime SHALL record the run in the Workflow_Timeout_Tracking_State with the timeout durations and `started_at` timestamp.
2. WHEN a run reaches a terminal state (the committed transition's `next_state.closed_at` is `Some`), THE Runtime SHALL remove the run from the Workflow_Timeout_Tracking_State. This cleanup happens in the lane's post-commit path (which has access to `CommitResult::Applied { new_state }`), not in the `DispatchPublisher`.
3. THE Workflow_Timeout_Tracking_State SHALL be keyed by `RunKey` to support efficient lookup and iteration by the scanner.
4. WHEN a `WorkflowExecutionTimedOut` command is successfully committed (run closed), THE Runtime SHALL remove the run from the Workflow_Timeout_Tracking_State.
5. THE Workflow_Timeout_Tracking_State SHALL store the `RunKey`, `workflow_execution_timeout` (optional), `workflow_run_timeout` (optional), `started_at` timestamp, and whether a retry policy is configured.

---

### Requirement 5: Workflow Timeout Scanner Background Task

**User Story:** As a Tokeira developer, I want a background scanner that periodically checks open runs for timeout violations, so that timeouts are detected without external polling.

#### Acceptance Criteria

1. THE Runtime SHALL run a background task (Workflow_Timeout_Scanner) that periodically iterates over all entries in the Workflow_Timeout_Tracking_State and checks each run against its configured timeouts.
2. THE Workflow_Timeout_Scanner SHALL use a configurable scan interval with a sensible default (e.g. 1 second).
3. WHEN the Workflow_Timeout_Scanner detects a timeout violation, THE Workflow_Timeout_Scanner SHALL submit the `WorkflowExecutionTimedOut` command to the owning run's lane using the same `submit` path used by other runtime commands.
4. THE Workflow_Timeout_Scanner SHALL process timeout violations in bounded batches per scan cycle to avoid starving other lane work.
5. THE Workflow_Timeout_Scanner SHALL capture the current wall-clock time once at the start of each scan cycle and use that single timestamp for all timeout comparisons within the cycle.

---

### Requirement 6: Workflow Timeout Scanner Configuration

**User Story:** As a Tokeira developer, I want the workflow timeout scanner to be configurable, so that operators can tune scan frequency and batch size for their workload.

#### Acceptance Criteria

1. THE Workflow_Timeout_Scanner SHALL accept a configuration struct with at least `scan_interval` (duration between scan cycles) and `max_timeouts_per_scan` (maximum timeout commands submitted per cycle).
2. THE `scan_interval` default SHALL be suitable for second-resolution timeout detection (e.g. 1 second).
3. THE `max_timeouts_per_scan` default SHALL be a reasonable batch bound (e.g. 100).

---

### Requirement 7: Workflow Timeout Scanner Lifecycle

**User Story:** As a Tokeira developer, I want the timeout scanner to start when the runtime is created and stop when explicitly shut down, so that timeout detection is active only while the runtime is serving.

#### Acceptance Criteria

1. WHEN the Runtime is created, THE Runtime SHALL spawn the Workflow_Timeout_Scanner as a background `tokio::spawn` task.
2. THE Runtime SHALL expose a cooperative shutdown method that cancels the Workflow_Timeout_Scanner via a CancellationToken and awaits its completion.
3. THE Workflow_Timeout_Scanner SHALL check the CancellationToken before each scan cycle and exit gracefully when cancellation is signaled.

---

### Requirement 8: Workflow Timeout Scanner Error Resilience

**User Story:** As a Tokeira developer, I want the workflow timeout scanner to be resilient to transient errors, so that temporary failures do not crash the scanner or leave timeouts undetected.

#### Acceptance Criteria

1. IF `submit` returns an error for a specific run (lane channel closed, OCC exhaustion), THEN THE Workflow_Timeout_Scanner SHALL log the error at warn level and continue processing remaining entries in the current scan cycle.
2. IF `submit` returns a kernel rejection (`Reject::RunClosed`, `Reject::MissingRun`), THEN THE Workflow_Timeout_Scanner SHALL log the rejection at debug level, remove the run from the Workflow_Timeout_Tracking_State, and continue processing remaining entries.
3. IF the Workflow_Timeout_Scanner encounters a transient error while iterating the tracking state, THEN THE Workflow_Timeout_Scanner SHALL log the error and continue to the next scan cycle rather than crashing.

---

### Requirement 9: Workflow Timeout Scanner Distributed Coordination (Deferred)

**User Story:** As a Tokeira developer, I want workflow timeout scanning to be scoped to owned shards in the future, so that multiple runtime nodes do not duplicate timeout work.

#### Acceptance Criteria

1. THE Workflow_Timeout_Scanner SHALL be designed to support shard-scoped scanning, where only runs for shards owned by the current runtime node are checked.
2. WHEN shard ownership changes, THE Workflow_Timeout_Scanner SHALL stop checking runs for relinquished shards and begin checking runs for newly acquired shards.
3. WHILE shard ownership is not yet implemented (Feature 11), THE Workflow_Timeout_Scanner SHALL check all tracked runs regardless of shard assignment. This is safe because workflow timeout scanning is non-authoritative — duplicate `WorkflowExecutionTimedOut` commands from multiple nodes are rejected harmlessly by the kernel.
