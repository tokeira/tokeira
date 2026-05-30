# Requirements Document

## Introduction

This spec implements OperatorService Nexus endpoint administration RPCs currently stubbed: get, create, update, delete, and list Nexus endpoints.

## Glossary

- **Nexus endpoint:** Operator-managed endpoint metadata used by Nexus task transport.
- **Endpoint version:** Conflict/version token for optimistic updates.
- **Endpoint registry:** Durable store of Nexus endpoint definitions.

## Target State

`ImplementedSubset`: Nexus endpoint admin registry only. This spec does not
claim completion of Nexus task polling, operation execution, cancellation, or
worker task transport conformance.

## Evidence From Current Code

- Proto messages inspected: OperatorService Nexus endpoint request/response messages.
- Current handlers: `get_nexus_endpoint`, `create_nexus_endpoint`, `update_nexus_endpoint`, `delete_nexus_endpoint`, `list_nexus_endpoints`.
- Existing runtime: Nexus task broker/transport exists separately from admin endpoint CRUD.
- Needed storage: endpoint id/name/version registry.

## Endpoint Field Policy

| Field group | Current state | Target policy | Error if unsupported | Storage impact |
|---|---|---|---|---|
| Endpoint id/name | Stubbed | Server-authored id, unique name | `ALREADY_EXISTS`, `NOT_FOUND` | Registry key/index |
| Spec/target fields | Stubbed | Validate and persist supported fields | `INVALID_ARGUMENT` or `UNIMPLEMENTED` | Registry value |
| Version token | Stubbed | Increment on update/delete | `ABORTED` on stale token | Registry CAS |
| Pagination | Stubbed | Stable name/id ordering | `INVALID_ARGUMENT` on bad token | Registry scan |

## Requirements

### Requirement 1: Nexus Endpoint CRUD

**User Story:** As an operator, I want to manage Nexus endpoints, so that workflows can target named Nexus services.

#### Acceptance Criteria

1. `CreateNexusEndpoint` SHALL create a new endpoint with server-authored id/version metadata.
2. `GetNexusEndpoint` SHALL return an existing endpoint by id or name according to proto request semantics.
3. `UpdateNexusEndpoint` SHALL apply supported mutable fields using optimistic version/conflict tokens.
4. `DeleteNexusEndpoint` SHALL remove or tombstone an endpoint using the required version check if present.
5. `ListNexusEndpoints` SHALL return endpoints with stable pagination.

### Requirement 2: Validation and Errors

**User Story:** As an operator, I want invalid endpoint mutations rejected before persistence, so that bad endpoint definitions do not break runtime dispatch.

#### Acceptance Criteria

1. Missing required endpoint target/spec fields SHALL return `INVALID_ARGUMENT`.
2. Duplicate endpoint names SHALL return `ALREADY_EXISTS`.
3. Missing endpoints SHALL return `NOT_FOUND`.
4. Stale version tokens SHALL return gRPC `ABORTED`.
5. Unsupported upstream fields SHALL return `UNIMPLEMENTED` before mutation.

### Requirement 3: Runtime Integration

**User Story:** As a workflow author, I want runtime Nexus dispatch to use operator-managed endpoints, so that endpoint admin changes affect future dispatches.

#### Acceptance Criteria

1. Newly created endpoints SHALL be visible to Nexus dispatch lookups after commit.
2. Deleted endpoints SHALL not be used for new Nexus dispatch.
3. Runtime reads SHALL use a trait boundary and SHALL NOT import OperatorService implementation types.
