# Design Document: Multi-Operation API Conformance

## Overview

`ExecuteMultiOperation` is stubbed and must not be implemented as sequential independent mutations. This design adds a same-run atomic admission path: the edge validates all operations, resolves the target run, and submits one runtime request that the lane applies as a single kernel transition and storage commit.

## Dependencies and Non-Goals

- Implements same-run start plus update/signal-style operation groups.
- Cross-run atomicity remains a non-goal; cross-run requests fail validation before mutation.
- Unknown future oneof variants fail validation until they are mapped to kernel commands.

## Architecture

```mermaid
flowchart LR
    Client --> Grpc["ExecuteMultiOperation"]
    Grpc --> Validate["translate and validate all ops"]
    Validate --> Runtime["execute_multi_operation"]
    Runtime --> Lane["single run lane"]
    Lane --> Kernel["single transition"]
    Kernel --> Store["single commit"]
    Store --> Response["ordered operation results"]
```

## Components and Interfaces

- `crates/tokeira-edge/src/grpc/workflow_service.rs`: replace stub.
- `crates/tokeira-edge/src/grpc/translate.rs`: free translation functions for multi-operation request/response.
- `crates/tokeira-edge/src/workflow_service.rs`: validation-first orchestration.
- `crates/tokeira-runtime`: add `execute_multi_operation` that routes all operations for one target run through one lane submit.
- `crates/tokeira-kernel`: apply a validated operation list as one transition, using existing start/update/signal command handlers where possible.
- `crates/tokeira-storage`: commit the resulting transition once, with existing OCC/fencing semantics.

## Correctness Properties

### Property 1: Validate Before Mutate

For any request containing an invalid, unknown, or cross-run operation, no runtime mutation method is called.

**Validates:** Requirements 1.2, 1.3, 2.1.

### Property 2: Atomic Result Ordering

For any successful supported request, response results match request operation order.

**Validates:** Requirements 1.4, 2.3.

### Property 3: No Partial Commit

Injected failure in any supported operation path leaves runtime state unchanged.

**Validates:** Requirements 2.1, 2.2, 2.3.

## Error Handling

| Condition | Error | gRPC status |
|---|---|---|
| Unknown operation variant | bad request | `INVALID_ARGUMENT` |
| Missing required operation field | bad request | `INVALID_ARGUMENT` |
| Cross-run operation group | bad request | `INVALID_ARGUMENT` |
| Conflict/already started | existing conflict error | existing mapped status |
| OCC/fencing conflict | existing commit conflict | existing mapped status |

## Testing Strategy

- Translator tests for every operation oneof variant.
- Mock runtime tests proving validation happens before mutation.
- Property tests for response ordering and no partial commit.
