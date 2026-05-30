# Requirements Document

## Introduction

This spec completes `DescribeWorkflowExecution` conformance for the `api-conformance-tracker` umbrella. The current handler is Partial: it returns basic execution metadata but does not populate execution configuration, pending activities, pending children, pending workflow task, callbacks, or pending Nexus operations documented in `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`.

## Glossary

- **Description snapshot:** A read-only runtime/projection view of one workflow run used to build `DescribeWorkflowExecutionResponse`.
- **Pending entity:** A workflow task, activity, child workflow, callback, or Nexus operation that is open in kernel state.
- **Execution config:** Task queue, workflow type, timeouts, and retry/versioning metadata needed by Temporal clients and operators.

## Target State

`ImplementedSubset`. This spec completes describe fields backed by durable
Tokeira state. Fields for unsupported runtime features, such as callbacks, are
not fabricated and remain empty only when the feature itself cannot exist.

## Evidence From Current Code

- Proto messages inspected: `DescribeWorkflowExecutionRequest`, `DescribeWorkflowExecutionResponse` in `proto/upstream/temporal/api/workflowservice/v1/request_response.proto`.
- Current handler: `WorkflowServiceGrpc::describe_workflow_execution` in `crates/tokeira-edge/src/grpc/workflow_service.rs`.
- Existing DTOs: `WorkflowExecutionDescription` in `crates/tokeira-edge/src/translate/mod.rs`.
- Unsupported-field entry: `DescribeWorkflowExecutionResponse` in `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`.
- Runtime/storage sources: `RunRepository::load_run`, kernel `WorkflowState`, visibility projection summaries.

## Response Field Policy

| Response field | Current state | Target policy | Source | Tests |
|---|---|---|---|---|
| `execution_config` | Not populated | Populate supported start config | Start attributes in run state | Unit + restart |
| `workflow_execution_info` | Partial | Preserve existing summary and fill missing stable fields | Visibility/run state | Regression |
| `pending_activities` | Not populated | Populate from pending activity snapshot | Kernel activity state and activity tracking | Property + restart |
| `pending_children` | Not populated | Populate when child workflow state exists | Kernel child state | Integration |
| `pending_workflow_task` | Not populated | Populate scheduled/started WFT fields when known | Kernel pending WFT state | Property |
| `callbacks` | Not modeled | Empty only because callbacks cannot currently exist | Future callback state | Explicit empty test |
| `pending_nexus_operations` | Not populated | Populate when pending Nexus state exists | Kernel/runtime Nexus state | Integration |

## Requirements

### Requirement 1: Complete DescribeWorkflowExecution Response

**User Story:** As an operator, I want `DescribeWorkflowExecution` to include all SDK-visible pending state, so that diagnostics and SDK tooling see Temporal-compatible execution metadata.

#### Acceptance Criteria

1. WHEN a workflow execution exists, THE Edge SHALL return `workflow_execution_info` using the authoritative run/projection state.
2. WHEN a workflow execution exists, THE Edge SHALL populate `execution_config` from the run's start attributes.
3. WHEN open activities exist, THE Edge SHALL populate `pending_activities` with activity id, type, task queue, attempt, state, last heartbeat details when available, and event link fields when available.
4. WHEN open child workflows exist, THE Edge SHALL populate `pending_children` with initiated event id, workflow id, run id when started, workflow type, and namespace data available in kernel state.
5. WHEN a workflow task is scheduled or started, THE Edge SHALL populate `pending_workflow_task` with state, attempt, scheduled/start event ids, and original scheduled/start times when available.
6. WHEN callbacks exist, THE Edge SHALL populate `callbacks`; IF callbacks are not yet modeled by the kernel, THE Edge SHALL return an empty list and tasks SHALL document the kernel/storage work needed before this field can be non-empty.
7. WHEN pending Nexus operations exist, THE Edge SHALL populate `pending_nexus_operations` from kernel/runtime state.
8. IF `run_id` is non-empty and malformed, THE Edge SHALL return gRPC `INVALID_ARGUMENT`.
9. IF the workflow execution cannot be resolved, THE Edge SHALL return gRPC `NOT_FOUND`.
10. THE Edge SHALL NOT use `EdgeError::Internal` for expected describe validation or lookup failures.

### Requirement 2: Snapshot Consistency

**User Story:** As an SDK client, I want describe data to come from one consistent run snapshot, so that pending-state fields do not contradict each other.

#### Acceptance Criteria

1. WHEN the Edge builds a describe response, THE pending entities SHALL be derived from the same loaded run state or projection version.
2. WHEN a pending entity is absent from the authoritative state, THE corresponding response list SHALL omit it rather than emitting default placeholder entries.
3. WHEN an event id is unknown because older state lacks a field, THE response SHALL leave that proto field default and SHALL NOT invent `0` as an authored event id.
4. THE serializer SHALL include regression coverage that a scheduled activity and pending workflow task appear together after a workflow schedules an activity.

### Requirement 3: Metrics and Errors

**User Story:** As an operator, I want describe failures to be observable with the right gRPC labels, so that dashboards distinguish invalid input from missing executions.

#### Acceptance Criteria

1. WHEN `DescribeWorkflowExecution` fails for malformed input, THE edge gRPC metrics SHALL record `invalid_argument`.
2. WHEN `DescribeWorkflowExecution` fails because the execution is missing, THE edge gRPC metrics SHALL record `not_found`.
3. WHEN describe succeeds, THE edge gRPC metrics SHALL record success using the existing method and namespace labels.
