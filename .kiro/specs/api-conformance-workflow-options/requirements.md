# Requirements Document

## Introduction

This spec implements `UpdateWorkflowExecutionOptions`, currently stubbed. It adds the kernel/runtime model needed for mutable workflow execution options, including `versioning_override`.

## Glossary

- **Workflow execution options:** Mutable per-execution options exposed by Temporal after workflow start.
- **Versioning override:** Worker versioning override attached to a workflow execution.
- **Options updated event:** Durable history event recording a change to execution options.

## Target State

`Implemented`. Workflow option changes commit as per-run transitions, update
deterministic run state, and emit `WorkflowExecutionOptionsUpdated` history
events with the authored changed fields.

## Evidence From Current Code

- Proto message inspected: `UpdateWorkflowExecutionOptionsRequest`.
- Current handler: `update_workflow_execution_options` stub.
- Related serializer note: `WorkflowExecutionOptionsUpdated.versioning_override` in `UNSUPPORTED_FIELDS.md`.
- Kernel: placeholder `VersioningOverride` model that this spec replaces with durable state.

## Option Field Policy

| Proto field/group | Current state | Target policy | Error if invalid | History impact |
|---|---|---|---|---|
| Execution reference | Stubbed | Validate and resolve | `INVALID_ARGUMENT` / `NOT_FOUND` | n/a |
| Request identity/id fields | Stubbed | Preserve if present in proto | validation errors | request dedupe if supported |
| `versioning_override` | Placeholder | Persist and apply to workflow task routing | validation errors for invalid enum values | `WorkflowExecutionOptionsUpdated` |
| Other supported options | Stubbed | Commit per-run transition | n/a | `WorkflowExecutionOptionsUpdated` |
| Empty update | Stubbed | Reject | `INVALID_ARGUMENT` | none |

## Requirements

### Requirement 1: UpdateWorkflowExecutionOptions

**User Story:** As an operator or SDK client, I want to update supported workflow execution options, so that runtime routing/versioning behavior can be adjusted safely.

#### Acceptance Criteria

1. WHEN the target execution exists, THE RPC SHALL update supported options by committing a per-run transition.
2. WHEN `versioning_override` is supplied, THE kernel SHALL persist it, THE runtime SHALL apply it to workflow task routing, and THE history serializer SHALL emit it.
3. WHEN future option fields are added to the proto, THE handler SHALL account for them in the field policy before accepting the request.
4. WHEN an option value is malformed or incompatible with current run state, THE Edge SHALL return `INVALID_ARGUMENT` or `FAILED_PRECONDITION` before mutation.
5. WHEN no option changes are supplied, THE Edge SHALL return `INVALID_ARGUMENT`.

### Requirement 2: Validation and Error Behavior

**User Story:** As an SDK client, I want invalid option updates rejected consistently, so that update calls are safe and debuggable.

#### Acceptance Criteria

1. Malformed non-empty `run_id` SHALL return `INVALID_ARGUMENT`.
2. Missing target execution SHALL return `NOT_FOUND`.
3. Stale or duplicate request ids SHALL follow existing request-dedupe behavior if the RPC carries a request id.
4. The handler SHALL not use `EdgeError::Internal` for expected validation failures.

### Requirement 3: History Fidelity

**User Story:** As a history consumer, I want option updates visible in workflow history, so that replay and diagnostics can observe changes.

#### Acceptance Criteria

1. Every successful update SHALL append `WorkflowExecutionOptionsUpdated`.
2. The event SHALL include all supported changed options.
3. Default fields SHALL not be serialized as fake authored values.
4. Updated option state SHALL survive process restart and affect subsequent workflow task dispatch.
