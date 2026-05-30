# Requirements Document

## Introduction

This spec implements `ExecuteMultiOperation`, currently stubbed. Temporal v1.31 uses this RPC for atomic multi-operation requests, primarily start plus update/signal-style combinations depending on proto variants.

## Glossary

- **Multi-operation:** A single RPC carrying multiple operation variants that must be validated and applied atomically.
- **Atomic admission:** Either every supported operation commits according to its semantics, or none do.
- **Operation result:** Per-operation response in the same order as the request.

## Target State

`Implemented`. `ExecuteMultiOperation` validates all operations, executes
supported same-run operation groups atomically through one runtime commit, and
returns per-operation results in request order.

## Evidence From Current Code

- Proto message inspected: `ExecuteMultiOperationRequest`.
- Current handler: `execute_multi_operation` stub.
- Runtime architecture: per-run lanes and per-run transitions; this spec uses a same-run transaction boundary and rejects cross-run groups as invalid.

## Operation Variant Policy

| Operation variant | Target policy | Reason |
|---|---|---|
| Start workflow | Implement | Opens the same-run transaction and may be followed by update/signal-style operations |
| Update workflow | Implement when targeting the same workflow execution | Applies in the same per-run transition |
| Signal-style variants | Implement when proto/version exposes them and they target the same workflow execution | Applies in the same per-run transition |
| Unknown/future variants | Reject with `INVALID_ARGUMENT` until mapped | No silent partial mutation |

## Requirements

### Requirement 1: Operation Variant Accounting

**User Story:** As an SDK client, I want every multi-operation variant explicitly handled, so that requests either commit atomically or fail before mutation.

#### Acceptance Criteria

1. WHEN a supported operation variant is supplied, THE Edge SHALL translate it to the corresponding internal request.
2. WHEN the operation list contains start plus update/signal-style operations for the same workflow execution, THE runtime SHALL apply them in a single per-run commit.
3. WHEN an operation is missing required fields, THE Edge SHALL return `INVALID_ARGUMENT`.
4. Response entries SHALL preserve request operation order.

### Requirement 2: Atomicity

**User Story:** As an SDK client, I want multi-operation execution to be atomic, so that partial commits do not violate Temporal semantics.

#### Acceptance Criteria

1. IF validation of any operation fails, no operation SHALL mutate runtime state.
2. IF commit of any operation fails after validation, the system SHALL not leave a partially applied multi-operation.
3. Atomic operation groups SHALL route through a single runtime method and one kernel transition for the target run.
4. Cross-run operation groups SHALL return `INVALID_ARGUMENT` before mutation unless a future cross-run transaction model is added.

### Requirement 3: Error and Metrics Behavior

**User Story:** As an operator, I want multi-operation failures labeled correctly, so that invalid requests and transaction conflicts are observable.

#### Acceptance Criteria

1. Unknown or unmapped operation variants SHALL map to `INVALID_ARGUMENT`.
2. Invalid operation payloads SHALL map to `INVALID_ARGUMENT`.
3. Conflict/already-started cases SHALL preserve existing start/signal conflict mappings.
