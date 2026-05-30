# Design Document: Workflow Options API Conformance

## Overview

`UpdateWorkflowExecutionOptions` is stubbed. This design adds an edge handler, runtime method, and kernel command for mutable workflow execution options, including a durable `versioning_override` model that affects workflow task routing.

## Dependencies and Non-Goals

- Depends on the start-field versioning override model from `api-conformance-start-fields`.
- Does not implement worker deployment administration; it stores and applies the per-workflow override value carried by this RPC.
- Must remain consistent with start-field versioning override semantics.

## History Event Contract

`WorkflowExecutionOptionsUpdated` is emitted only for committed option changes.
The serializer includes exactly the changed options. `versioning_override` is
present when authored through start or update options and absent when unchanged.

## Architecture

```mermaid
flowchart LR
    Client --> Grpc["update_workflow_execution_options"]
    Grpc --> Edge["validate and translate"]
    Edge --> Runtime["submit per-run command"]
    Runtime --> Kernel["UpdateExecutionOptions"]
    Kernel --> History["WorkflowExecutionOptionsUpdated"]
```

## Components and Interfaces

- `crates/tokeira-edge/src/grpc/workflow_service.rs`: replace stub with handler.
- `crates/tokeira-edge/src/grpc/translate.rs`: add free request/response translation functions.
- `crates/tokeira-edge/src/workflow_service.rs`: resolve execution and submit command.
- `crates/tokeira-kernel/src/command.rs`: add/update execution options command carrying `FieldChange<T>` values.
- `crates/tokeira-kernel/src/state.rs`: persist execution options including versioning override.
- `crates/tokeira-runtime`: apply changed options to subsequent dispatch/routing decisions.
- `crates/tokeira-edge/src/translate/history_serializer.rs`: serialize changed option fields.

## Correctness Properties

### Property 1: Options Commit Fidelity

For any supported option subset, the committed state and emitted history event contain exactly those changes.

**Validates:** Requirements 1.1, 3.1, 3.2.

### Property 2: Versioning Override Fidelity

For any authored `versioning_override`, committed state, emitted history, and subsequent routing metadata reflect the requested value.

**Validates:** Requirements 1.2, 1.3, 1.4.

### Property 3: Expected Error Mapping

Malformed run id, missing execution, and empty changes map to documented gRPC statuses.

**Validates:** Requirements 1.5, 2.1, 2.2, 2.4.

## Error Handling

| Condition | Error | gRPC status |
|---|---|---|
| Malformed run id | bad request | `INVALID_ARGUMENT` |
| Missing execution | workflow not found | `NOT_FOUND` |
| No changes | bad request | `INVALID_ARGUMENT` |
| Incompatible option value | failed precondition | `FAILED_PRECONDITION` |

## Testing Strategy

- Translator tests for every request field.
- Kernel tests for options state and event emission.
- Serializer tests for `WorkflowExecutionOptionsUpdated`.
- gRPC tests for missing execution, malformed run id, no changes, and incompatible option values.
- Restart/recovery tests for persisted option state and dispatch behavior.
