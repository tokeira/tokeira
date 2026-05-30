# Requirements Document

## Introduction

This spec completes `UpdateWorkflowExecution` and `PollWorkflowExecutionUpdate` response conformance. Current handlers are Partial; `UNSUPPORTED_FIELDS.md` documents missing `update_ref` and `stage` fields in `UpdateWorkflowExecutionResponse`.

## Glossary

- **Update ref:** Stable reference identifying a workflow update request and target execution.
- **Update stage:** Server-known lifecycle position for an update, such as admitted, accepted, completed, or rejected.
- **Protocol transport:** The existing message-driven update path between edge, runtime, and worker.

## Target State

`ImplementedSubset`. The RPCs expose stable refs and stages for update lifecycle
states Tokeira can recover. Unknown update ids return a chosen, documented
status rather than ambiguous pending behavior.

## Evidence From Current Code

- Proto messages inspected: `UpdateWorkflowExecutionRequest`, `UpdateWorkflowExecutionResponse`, `PollWorkflowExecutionUpdateRequest`, `PollWorkflowExecutionUpdateResponse`.
- Current handlers: `update_workflow_execution`, `poll_workflow_execution_update`.
- Unsupported-field entry: `UpdateWorkflowExecutionResponse` in `UNSUPPORTED_FIELDS.md`.
- Runtime/kernel: update registry, update protocol messages, update history events.

## Update Stage Policy

| Tokeira state | Temporal stage policy | Durability requirement |
|---|---|---|
| Admitted to runtime | Return admitted/pending stage | Registry snapshot must survive wait path |
| Accepted by worker | Return accepted stage | Committed history or durable registry |
| Completed | Return completed stage + outcome | Committed history |
| Rejected | Return rejected stage + failure | Committed history or durable registry |
| Timed out/canceled | Return terminal failure or `NOT_FOUND` according to chosen behavior | Durable cleanup record if pollable after restart |
| Unknown update id | Return `NOT_FOUND` | No mutation |

## Requirements

### Requirement 1: Update Response Metadata

**User Story:** As an SDK client, I want update responses to carry `update_ref` and stage, so that clients can poll and reason about update lifecycle.

#### Acceptance Criteria

1. WHEN `UpdateWorkflowExecution` admits an update, THE response SHALL populate `update_ref`.
2. WHEN an update reaches an accepted/completed/rejected stage before the call returns, THE response SHALL populate `stage` consistently with the result.
3. WHEN the update is still pending, THE response SHALL expose the best known stage rather than defaulting silently.
4. IF a stage cannot be represented because runtime lacks lifecycle state, THE task SHALL add runtime state rather than inventing edge-only values.

### Requirement 2: Poll Update Consistency

**User Story:** As an SDK client, I want `PollWorkflowExecutionUpdate` to observe the same lifecycle state as `UpdateWorkflowExecution`, so that polling is deterministic.

#### Acceptance Criteria

1. WHEN polling an existing update, THE response SHALL return the current outcome or stage using the same update id.
2. WHEN polling an unknown update id, THE Edge SHALL return `NOT_FOUND` or documented Temporal-compatible pending behavior.
3. WHEN `run_id` is non-empty and malformed, THE Edge SHALL return `INVALID_ARGUMENT`.
4. Polling SHALL NOT submit workflow mutations.

### Requirement 3: Protocol Compatibility

**User Story:** As a worker implementation, I want update protocol messages to remain compatible, so that lifecycle metadata does not break the existing update transport.

#### Acceptance Criteria

1. Existing accepted/completed/rejected protocol bodies SHALL continue to translate.
2. New lifecycle metadata SHALL be derived from runtime update registry state or committed history, not from client-supplied guesses.
3. Timeout and cancellation paths SHALL clean up update registry state and expose a terminal or absent state consistently.
