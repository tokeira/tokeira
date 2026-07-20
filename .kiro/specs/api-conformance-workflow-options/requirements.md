# Requirements Document

## Introduction

This spec implements `UpdateWorkflowExecutionOptions` and the shared mutation used by
batch updates and post-reset operations. It owns the durable, serialized update of
mutable workflow execution options, including `versioning_override`.

## Glossary

- **Workflow execution options:** Mutable per-execution options exposed by Temporal after workflow start.
- **Versioning override:** Worker versioning override attached to a workflow execution.
- **Implied pinned override:** The v0.32 pinned form whose version is omitted and must be
  resolved from the target run's effective pinned deployment.
- **Options updated event:** Durable history event recording a change to execution options.

## Target State

`Implemented`. Workflow option changes commit as per-run transitions, update
deterministic run state, and emit `WorkflowExecutionOptionsUpdated` history
events with the authored changed fields.

## Evidence From Current Code

- Proto message inspected: `UpdateWorkflowExecutionOptionsRequest`.
- The direct RPC, batch operation, post-reset operation, kernel event, replay path, and
  history serialization are implemented.
- Temporal resolves an implied pinned override while holding the target workflow lease,
  then validates, merges, applies, and persists the concrete value in the same serialized
  mutation (`service/history/api/updateworkflowoptions/api.go @ v1.31.0`).
- `mergeWorkflowExecutionOptions` treats an empty field mask as a successful no-op;
  `MergeAndApply` emits no event when the merged options equal current state
  (`service/history/api/updateworkflowoptions/api.go @ v1.31.0`).

## Option Field Policy

| Proto field/group | Current state | Target policy | Error if invalid | History impact |
|---|---|---|---|---|
| Execution reference | Implemented | Validate and resolve | `INVALID_ARGUMENT` / `NOT_FOUND` | n/a |
| Request identity/id fields | Implemented | Preserve identity when present; this RPC has no request id | field-specific validation | event identity |
| `versioning_override` | Implemented | Persist concrete Set/Clear changes and apply them to workflow-task routing | `INVALID_ARGUMENT` for malformed values; `FAILED_PRECONDITION` for an implied pin on a non-pinned run or explicit pin outside the task queue | `WorkflowExecutionOptionsUpdated` only when state changes |
| Other supported options | Tracked separately | Account for each field before accepting it | field-specific | `WorkflowExecutionOptionsUpdated` when supported and changed |
| Empty update mask | Implemented | Successful no-op returning current options | none | none |

## Requirements

### Requirement 1: UpdateWorkflowExecutionOptions

**User Story:** As an operator or SDK client, I want to update supported workflow execution options, so that runtime routing/versioning behavior can be adjusted safely.

#### Acceptance Criteria

1. WHEN the target execution exists, THE RPC SHALL update supported options by committing a per-run transition.
2. WHEN `versioning_override` is supplied, THE kernel SHALL persist it, THE runtime SHALL apply it to workflow task routing, and THE history serializer SHALL emit it.
3. WHEN future option fields are added to the proto, THE handler SHALL account for them in the field policy before accepting the request.
4. IF an option value is structurally malformed, THEN THE Edge SHALL return
   `INVALID_ARGUMENT` before submission. IF an explicit pinned version is not a member
   of the run's workflow task queue, THEN THE runtime SHALL return
   `FAILED_PRECONDITION` before committing the run transition.
5. WHEN the update mask is empty, THE RPC SHALL return the run's current workflow
   execution options as a successful no-op and SHALL NOT append history.
6. WHEN a pinned override requests PINNED behavior but omits its version, THE pure kernel
   transition SHALL resolve the concrete version from the run's effective deployment in
   lane order. IF the run's effective behavior is not PINNED or no effective deployment
   exists, THEN the transition SHALL reject with `FAILED_PRECONDITION` and the exact
   v1.31.0 reason naming the workflow id and effective behavior.
7. WHEN an implied pinned override succeeds, THE response, committed state, and history
   event SHALL contain the concrete resolved pinned version, not the unresolved marker.
8. WHEN the requested concrete option state equals the current option state, THE RPC
   SHALL return current options as a successful no-op and SHALL NOT append history.
9. State-dependent implied-pin resolution SHALL occur inside the serialized per-run
   transition. The Edge SHALL NOT resolve it from a separately loaded snapshot.

### Requirement 2: Validation and Error Behavior

**User Story:** As an SDK client, I want invalid option updates rejected consistently, so that update calls are safe and debuggable.

#### Acceptance Criteria

1. Malformed non-empty `run_id` SHALL return `INVALID_ARGUMENT`.
2. Missing target execution SHALL return `NOT_FOUND`.
3. Stale or duplicate request ids SHALL follow existing request-dedupe behavior if the RPC carries a request id.
4. The handler SHALL not use `EdgeError::Internal` for expected validation failures.
5. A kernel rejection for an unresolved implied pin SHALL map to gRPC
   `FAILED_PRECONDITION`; it SHALL NOT be collapsed into `INTERNAL` or
   `INVALID_ARGUMENT`.

### Requirement 3: History Fidelity

**User Story:** As a history consumer, I want option updates visible in workflow history, so that replay and diagnostics can observe changes.

#### Acceptance Criteria

1. Every successful update that changes option state SHALL append
   `WorkflowExecutionOptionsUpdated`; successful no-ops SHALL append no event.
2. The event SHALL include all supported changed options.
3. Default fields SHALL not be serialized as fake authored values.
4. Updated option state SHALL survive process restart and affect subsequent workflow task dispatch.
5. `WorkflowExecutionOptionsUpdated.identity` SHALL carry the initiating identity for
   direct and batch updates; reset-authored operations use the identity supplied by the
   reset contract.
