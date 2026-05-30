# Design Document: Legacy Visibility API Conformance

## Overview

The modern list/count visibility APIs exist, while legacy visibility RPCs are stubbed. This design adapts legacy filters, scan, archived listing, and search attribute catalog reads onto the existing visibility query surface.

## Dependencies and Non-Goals

- Depends on the existing visibility projection and search attribute catalog.
- Does not add a separate archive store; archived listing is a compatibility wrapper over the modern visibility query path.
- Visibility is projection-backed and may lag history; response consistency must document the same lag semantics as modern visibility APIs.

## Page Token Policy

Legacy visibility page tokens must encode request shape, offset/cursor, and a
version. A token presented with different namespace/filter/page-size inputs is
invalid and returns `INVALID_ARGUMENT`.

## Architecture

```mermaid
flowchart LR
    Client --> Grpc["legacy visibility RPC"]
    Grpc --> Translate["legacy filter translation"]
    Translate --> Visibility["VisibilityApi"]
    Visibility --> Response["WorkflowExecutionInfo list"]
```

## Components and Interfaces

- `crates/tokeira-edge/src/grpc/workflow_service.rs`: replace stubs with handlers.
- `crates/tokeira-edge/src/grpc/translate.rs`: add free translation functions for legacy list/scan requests.
- `crates/tokeira-projection`: expose required filters through the existing visibility API or add typed legacy filter methods.
- `crates/tokeira-edge/src/operator_service.rs` or visibility metadata module: expose search attribute catalog.

## Correctness Properties

### Property 1: Status Partition

Open list never returns closed executions; closed list never returns open executions.

**Validates:** Requirements 1.1, 1.2.

### Property 2: Filter Equivalence

Supported legacy filters produce the same result set as equivalent modern visibility queries.

**Validates:** Requirements 1.3, 2.3.

### Property 3: Archived Wrapper Equivalence

Archived visibility uses the same projection-backed query semantics as the equivalent modern visibility request and never silently ignores filters.

**Validates:** Requirement 2.1.

## Error Handling

| Condition | Error | gRPC status |
|---|---|---|
| Invalid filter | `BadRequest` | `INVALID_ARGUMENT` |
| Invalid page token | `BadRequest` | `INVALID_ARGUMENT` |
| Missing namespace | `NamespaceNotFound` | `NOT_FOUND` |

## Testing Strategy

- Translator tests for each legacy filter oneof.
- Property tests comparing legacy filters to modern query results.
- Pagination token round-trip tests.
- Catalog tests for search attributes.
