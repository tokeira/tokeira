# Requirements Document

## Introduction

This spec completes field-level conformance for `RespondWorkflowTaskCompleted`. The handler is Partial and the unsupported fields are listed in `UNSUPPORTED_FIELDS.md`: sticky attributes, SDK metadata, metering metadata, deployment, versioning behavior, and complete `return_new_workflow_task` semantics.

## Glossary

- **WFT completion:** A worker response that completes a workflow task and may carry commands, metadata, and polling preferences.
- **Sticky attributes:** Worker-provided sticky task queue data used for cache-affine workflow task dispatch.
- **Return-new-WFT:** Temporal behavior where a completion response can include the next workflow task immediately.

## Target State

`Implemented`. Completion metadata, sticky attributes, metering metadata,
worker version/deployment fields, versioning behavior, and
`return_new_workflow_task` are translated, persisted where durable, and applied
by runtime dispatch where behavioral.

## Evidence From Current Code

- Proto message inspected: `RespondWorkflowTaskCompletedRequest`.
- Current handler: `respond_workflow_task_completed`.
- Current DTO/kernel request: `WorkflowTaskCompletedRequest`.
- Unsupported-field entry: `RespondWorkflowTaskCompletedRequest` in `UNSUPPORTED_FIELDS.md`.
- Related runtime areas: query consistency model, broker, worker registry, versioning rule store.

## Completion Field Policy

| Proto field | Current state | Target policy | Error if invalid | Persistence/history impact |
|---|---|---|---|---|
| `task_token`, `commands`, `identity` | Supported | Preserve | existing token/command errors | Kernel transition/history |
| `sdk_metadata` | Not supported | Thread raw metadata if accepted | n/a | `WorkflowTaskCompleted` event |
| `worker_version_stamp` | Deprecated/partial | Preserve build id and use as routing metadata where applicable | validation errors only | History/routing metadata |
| `sticky_attributes` | Not supported | Persist sticky task queue attributes and update routing | validation errors only | Sticky routing state |
| `return_new_workflow_task` | Partial | Implement inline next-WFT delivery when safely available | n/a | Runtime response |
| `metering_metadata` | Not supported | Preserve as informational completion metadata | n/a | `WorkflowTaskCompleted` event |
| `deployment`, `versioning_behavior` | Not supported | Persist dispatch metadata and apply to routing/versioning behavior | validation errors only | Worker deployment/versioning state |

## Requirements

### Requirement 1: Completion Metadata Preservation

**User Story:** As an SDK worker, I want completion metadata preserved, so that server history and diagnostics reflect worker behavior.

#### Acceptance Criteria

1. WHEN `sdk_metadata` is present, THE Edge SHALL serialize and thread it into `WorkflowTaskCompletedRequest` and history.
2. WHEN `worker_version_stamp` or deployment fields are present, THE Edge SHALL preserve them as completion metadata and THE runtime SHALL apply supported routing/versioning effects.
3. WHEN `metering_metadata` is present, THE Edge SHALL preserve it as informational completion metadata.
4. Deprecated `binary_checksum` SHALL remain accepted only for compatibility and SHALL NOT drive new behavior.

### Requirement 2: Sticky and Versioning Behavior

**User Story:** As an SDK worker, I want sticky and versioning fields handled deterministically, so that cache and deployment features do not behave unpredictably.

#### Acceptance Criteria

1. WHEN `sticky_attributes` are present, THE runtime SHALL update sticky routing for the workflow execution.
2. WHEN `versioning_behavior` is present, THE runtime SHALL persist and apply the requested versioning behavior to subsequent workflow task dispatch.
3. THE handler SHALL NOT silently ignore non-default sticky/versioning fields.

### Requirement 3: Return-New-Workflow-Task Semantics

**User Story:** As an SDK worker, I want `return_new_workflow_task` to match Temporal semantics, so that worker poll loops can use completion response optimization safely.

#### Acceptance Criteria

1. WHEN `return_new_workflow_task` is false, THE response SHALL preserve existing behavior.
2. WHEN `return_new_workflow_task` is true and an immediately available WFT exists for the same worker/task queue, THE response SHALL include it.
3. WHEN no immediate WFT exists, THE response SHALL return without inventing an empty started task.
4. Returned inline WFTs SHALL carry the same token, history, sticky, and versioning metadata as a normal poll response.

### Requirement 4: Error and Token Validation

**User Story:** As an operator, I want invalid WFT completions rejected consistently, so that workers cannot corrupt history.

#### Acceptance Criteria

1. Malformed task tokens SHALL return `INVALID_ARGUMENT`.
2. Stale shard epoch or ownership failures SHALL continue to return the existing not-owner error.
3. Invalid non-default fields SHALL return `INVALID_ARGUMENT` or `FAILED_PRECONDITION` before command mutation.
