# Requirements Document: Nexus Operation Dispatch

## Introduction

This document captures the requirements for Feature 9 of the runtime-complete-implementation master spec: Nexus Operation Dispatch. The runtime must handle outbound Nexus operations triggered by `DispatchOp::ScheduleNexusOperation` and `DispatchOp::CancelNexusOperation`, and deliver resolution results back to the originating run via `Command::NexusOperationResolved`.

Nexus operations are cross-namespace service invocations through typed contracts on named endpoints. Unlike all previous dispatch ops (child workflows, external signals, external cancels), Nexus dispatch requires outbound HTTP calls to external services — this is the first feature that introduces network I/O from the runtime's dispatch publisher.

The kernel already handles the authoritative state transitions:
- `WorkflowCommand::ScheduleNexusOperation` emits `DispatchOp::ScheduleNexusOperation` and inserts a `PendingNexusOperation` entry in the originator's `pending_nexus_operations` map.
- `WorkflowCommand::CancelNexusOperation` emits `DispatchOp::CancelNexusOperation` and emits a `NexusOperationCancelRequested` history event.
- `Command::NexusOperationResolved` processes the resolution (Started, Completed, Failed, Canceled, TimedOut), emits the appropriate history event, and removes the entry from `pending_nexus_operations` (except for Started, which marks the entry as started).

The runtime's job is orchestration: translate dispatch ops into HTTP calls to Nexus endpoints, and deliver resolution results back to the originator. The `RuntimeDispatchPublisher` currently logs Nexus dispatch ops as stubs (falling through to the `other =>` catch-all arm). This feature replaces those stubs.

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing) and Feature 2 (Activity Pump), both already implemented.

The authoritative specification is [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md): Nexus resolution is authoritative (kernel command producing history events); the HTTP call is a side effect.

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication. Performs I/O but delegates state transition logic to the Kernel.
- **Nexus_Operation**: A cross-namespace service invocation through typed contracts, dispatched by the runtime on behalf of the kernel via HTTP to a named endpoint.
- **Nexus_Endpoint**: A named network address that hosts one or more Nexus services. The runtime resolves endpoint names to network addresses via the Nexus_Endpoint_Registry.
- **Nexus_Endpoint_Registry**: An in-memory map maintained by the runtime that maps Nexus endpoint names to network addresses and service metadata.
- **Nexus_HTTP_Client**: An abstraction (trait) over the HTTP transport used to dispatch Nexus operations and cancellation requests to external endpoints. Allows test implementations to mock network I/O.
- **RuntimeDispatchPublisher**: The `DispatchPublisher` implementation in `tokeira-runtime` that forwards dispatch ops to the appropriate subsystem after a transition is committed.
- **PendingNexusOperation**: Kernel state tracking a Nexus operation that has been scheduled but not yet reached a terminal state, keyed by operation_id in `WorkflowState.pending_nexus_operations`.
- **NexusResolution**: The terminal or intermediate state of a Nexus operation: Started, Completed, Failed, Canceled, or TimedOut.
- **Nexus_Timeout_Scanner**: A background task that periodically checks pending Nexus operations for schedule_to_close_timeout violations and submits TimedOut resolutions.
- **Run_Key**: Composite storage key identifying a single workflow run, used for lane routing and storage lookups.
- **Lane**: A single-thread serial command processor hosting many run actors.

## Requirements

---

### Requirement 1: Schedule Nexus Operation Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle `DispatchOp::ScheduleNexusOperation`, so that workflows can invoke cross-namespace Nexus services via HTTP.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::ScheduleNexusOperation`, THE RuntimeDispatchPublisher SHALL resolve the Nexus endpoint name via the Nexus_Endpoint_Registry and dispatch the operation via the Nexus_HTTP_Client to the resolved network address.
2. THE outbound Nexus HTTP request SHALL carry the operation_id, endpoint, service, operation name, input payload, and schedule_to_close_timeout from the dispatch op.
3. WHEN the Nexus HTTP response indicates synchronous completion, THE RuntimeDispatchPublisher SHALL submit a `Command::NexusOperationResolved` with a `NexusResolution::Completed` containing the result payload to the originating run.
4. WHEN the Nexus HTTP response indicates synchronous failure, THE RuntimeDispatchPublisher SHALL submit a `Command::NexusOperationResolved` with a `NexusResolution::Failed` containing the failure message to the originating run.
5. WHEN the Nexus HTTP response indicates asynchronous acceptance, THE RuntimeDispatchPublisher SHALL submit a `Command::NexusOperationResolved` with a `NexusResolution::Started` to the originating run.
6. THE RuntimeDispatchPublisher SHALL process each `ScheduleNexusOperation` dispatch op in a spawned async task, so that one slow or failing Nexus call does not block other dispatch ops in the same batch.

### Requirement 2: Originator Identity Propagation for Schedule

**User Story:** As a Tokeira developer, I want the `DispatchOp::ScheduleNexusOperation` to carry the originator's run_key, so that the RuntimeDispatchPublisher can deliver resolution results back to the correct run.

#### Acceptance Criteria

1. THE `DispatchOp::ScheduleNexusOperation` SHALL carry an `originator_run_key: RunKey` field identifying the workflow run that scheduled the Nexus operation.
2. WHEN the kernel processes a `WorkflowCommand::ScheduleNexusOperation`, THE Kernel SHALL populate the `originator_run_key` field on the emitted `DispatchOp::ScheduleNexusOperation` from `builder.state.run_key`.
3. THE `DispatchOp::ScheduleNexusOperation` SHALL carry a `scheduled_event_id: i64` field containing the event ID of the emitted `NexusOperationScheduled` history event.
4. WHEN the kernel processes a `WorkflowCommand::ScheduleNexusOperation`, THE Kernel SHALL populate the `scheduled_event_id` field on the emitted `DispatchOp::ScheduleNexusOperation` from the emitted history event ID.
5. THE `DispatchOp::ScheduleNexusOperation` SHALL carry a `scheduled_at: OffsetDateTime` field containing the `happened_at` timestamp of the emitted `NexusOperationScheduled` history event, so the timeout tracker uses the authoritative event time rather than the wall-clock time at dispatch publication.

### Requirement 3: Cancel Nexus Operation Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle `DispatchOp::CancelNexusOperation`, so that workflows can cancel pending Nexus operations.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::CancelNexusOperation`, THE RuntimeDispatchPublisher SHALL send a cancellation HTTP request to the Nexus endpoint for the identified operation via the Nexus_HTTP_Client.
2. IF the cancellation HTTP request fails or the Nexus endpoint reports that the operation has already completed, THEN THE RuntimeDispatchPublisher SHALL treat the failure as a harmless no-op and log at debug level.
3. IF the cancellation HTTP request succeeds, THEN THE RuntimeDispatchPublisher SHALL submit a `Command::NexusOperationResolved` with a `NexusResolution::Canceled` to the originating run.
4. THE RuntimeDispatchPublisher SHALL process each `CancelNexusOperation` dispatch op in a spawned async task.

### Requirement 4: Originator Identity Propagation for Cancel

**User Story:** As a Tokeira developer, I want the `DispatchOp::CancelNexusOperation` to carry the originator's run_key and operation_id, so that the RuntimeDispatchPublisher can identify the target Nexus endpoint and deliver cancellation results back to the correct run.

#### Acceptance Criteria

1. THE `DispatchOp::CancelNexusOperation` SHALL carry an `originator_run_key: RunKey` field identifying the workflow run that owns the Nexus operation.
2. THE `DispatchOp::CancelNexusOperation` SHALL carry an `operation_id: String` field identifying the Nexus operation to cancel.
3. THE `DispatchOp::CancelNexusOperation` SHALL carry `endpoint: String` and `service: String` fields so the RuntimeDispatchPublisher can resolve the target Nexus endpoint without an additional storage read.
4. WHEN the kernel processes a `WorkflowCommand::CancelNexusOperation`, THE Kernel SHALL populate the `originator_run_key`, `operation_id`, `endpoint`, and `service` fields on the emitted `DispatchOp::CancelNexusOperation` from `builder.state.run_key` and the matching `PendingNexusOperation` entry.

### Requirement 5: Nexus Endpoint Registry

**User Story:** As a Tokeira developer, I want the runtime to maintain a Nexus endpoint registry, so that endpoint names can be resolved to network addresses for HTTP dispatch.

#### Acceptance Criteria

1. THE Runtime SHALL maintain a Nexus_Endpoint_Registry that maps endpoint names to network addresses and service metadata.
2. THE Nexus_Endpoint_Registry SHALL be an in-memory map for the initial implementation, with entries configurable at runtime construction time.
3. WHEN a `DispatchOp::ScheduleNexusOperation` references an endpoint name that is not present in the Nexus_Endpoint_Registry, THE RuntimeDispatchPublisher SHALL submit a `Command::NexusOperationResolved` with a `NexusResolution::Failed` resolution containing a descriptive "endpoint not found" message to the originating run.
4. WHEN a `DispatchOp::CancelNexusOperation` references an endpoint name that is not present in the Nexus_Endpoint_Registry, THE RuntimeDispatchPublisher SHALL treat the unresolved endpoint as a harmless no-op and log at warn level.

### Requirement 6: Nexus HTTP Client Abstraction

**User Story:** As a Tokeira developer, I want the Nexus HTTP transport to be abstracted behind a trait, so that tests can mock outbound network I/O without requiring real HTTP endpoints.

#### Acceptance Criteria

1. THE Runtime SHALL define a `NexusHttpClient` trait with methods for dispatching a Nexus operation (start) and canceling a Nexus operation.
2. THE `NexusHttpClient::start_operation` method SHALL accept the resolved network address, operation_id, service, operation name, input payload, and schedule_to_close_timeout, and SHALL return a result indicating synchronous completion, synchronous failure, or asynchronous acceptance.
3. THE `NexusHttpClient::cancel_operation` method SHALL accept the resolved network address, operation_id, and service, and SHALL return a result indicating success or failure. The operation name is not needed for cancellation — the Nexus cancel protocol identifies by operation_id.
4. THE RuntimeDispatchPublisher SHALL accept a `NexusHttpClient` implementation at construction time and use it for all outbound Nexus HTTP calls.

### Requirement 7: Nexus Operation Timeout Handling

**User Story:** As a Tokeira developer, I want the runtime to detect Nexus operation schedule-to-close timeouts, so that Nexus operations that exceed their configured timeout are resolved as timed out.

#### Acceptance Criteria

1. WHEN a Nexus operation has a configured `schedule_to_close_timeout` and the elapsed time since the `NexusOperationScheduled` event exceeds the timeout, THE Runtime SHALL submit a `Command::NexusOperationResolved` with a `NexusResolution::TimedOut` to the originating run.
2. THE Runtime SHALL maintain runtime-local tracking state for pending Nexus operations that have a `schedule_to_close_timeout`, recording the originator run_key, operation_id, scheduled_event_id, schedule_to_close_timeout, and the authoritative `scheduled_at` timestamp from the committed transition (not the wall-clock time at dispatch publication).
3. THE Runtime SHALL run a Nexus_Timeout_Scanner background task that periodically checks tracked Nexus operations for timeout violations.
4. THE Nexus_Timeout_Scanner SHALL use a configurable scan interval.
5. THE Nexus_Timeout_Scanner SHALL use a configurable maximum number of timeouts processed per scan cycle.
6. WHEN a `Command::NexusOperationResolved` with a TimedOut resolution is rejected by the kernel (e.g., `UnknownNexusOperation` because the operation was already resolved), THE Runtime SHALL treat the rejection as a harmless no-op and remove the entry from tracking state.

### Requirement 8: Nexus Timeout Tracking Lifecycle

**User Story:** As a Tokeira developer, I want Nexus timeout tracking entries to be added when operations are scheduled and removed when operations are resolved, so that the tracking state stays consistent with kernel state.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::ScheduleNexusOperation` with a non-None `schedule_to_close_timeout`, THE Runtime SHALL insert a tracking entry into the Nexus timeout tracking state.
2. WHEN a `Command::NexusOperationResolved` is successfully committed for a Nexus operation with a terminal resolution (Completed, Failed, Canceled, or TimedOut), THE Runtime SHALL remove the corresponding tracking entry from the Nexus timeout tracking state. The `Started` resolution is non-terminal and SHALL NOT remove the tracking entry.
3. WHEN a run reaches a terminal state (closed), THE Runtime SHALL remove all Nexus timeout tracking entries for that run.

### Requirement 9: Nexus Dispatch Error Handling

**User Story:** As a Tokeira developer, I want Nexus dispatch errors to be handled gracefully, so that transient failures do not leave the originating workflow in an inconsistent state.

#### Acceptance Criteria

1. IF the Nexus_HTTP_Client returns a transient error (network timeout, connection refused, HTTP 5xx) for a start_operation call, THEN THE RuntimeDispatchPublisher SHALL submit a `Command::NexusOperationResolved` with a `NexusResolution::Failed` containing a descriptive error message to the originating run.
2. IF the resolution delivery (`Command::NexusOperationResolved`) to the originating run fails (lane channel closed, OCC exhaustion), THEN THE RuntimeDispatchPublisher SHALL log at warn level with sufficient context for operational diagnosis.
3. IF the Nexus_HTTP_Client returns a transient error for a cancel_operation call, THEN THE RuntimeDispatchPublisher SHALL treat the failure as a harmless no-op and log at warn level.

### Requirement 10: Nexus Timeout Scanning Is Non-Authoritative

**User Story:** As a Tokeira developer, I want Nexus timeout scanning to be non-authoritative, so that duplicate or stale timeout submissions are harmless.

#### Acceptance Criteria

1. THE Nexus_Timeout_Scanner SHALL NOT modify authoritative state directly; the authoritative transition happens when the Kernel processes the `NexusOperationResolved` command with a TimedOut resolution.
2. WHEN a `NexusOperationResolved` command with a TimedOut resolution is delivered for an operation that has already been resolved, THE Kernel SHALL reject it with `UnknownNexusOperation`, and THE Runtime SHALL treat that rejection as a harmless no-op.
3. WHEN a `NexusOperationResolved` command with a TimedOut resolution is delivered for a run that is already closed, THE Kernel SHALL reject it with `RunClosed`, and THE Runtime SHALL treat that rejection as a harmless no-op.
