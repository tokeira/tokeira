# Requirements Document

## Introduction

This spec implements legacy visibility RPCs currently stubbed: `ListOpenWorkflowExecutions`, `ListClosedWorkflowExecutions`, `ListArchivedWorkflowExecutions`, `ScanWorkflowExecutions`, and `GetSearchAttributes`.

## Glossary

- **Legacy visibility:** Pre-query visibility RPCs that filter by status/time/window fields rather than a SQL-like query string.
- **Archived visibility:** Legacy archived listing surface mapped to the modern visibility query path for Tokeira's current projection-backed store.
- **Search attributes catalog:** The set of indexed search attribute names and types.

## Target State

`Implemented`. Open, closed, archived, scan, and search-attribute catalog RPCs
are implemented as thin adapters over the modern visibility/query catalog
surface.

## Evidence From Current Code

- Proto messages inspected: legacy list/scan/get-search-attributes requests.
- Current handlers: legacy visibility methods in `workflow_service.rs`.
- Existing API: `VisibilityApi`, `ListWorkflowExecutions`, `CountWorkflowExecutions`.
- Related OperatorService methods: `AddSearchAttributes`, `ListSearchAttributes`.

## Legacy Filter Policy

| Filter/request | Current state | Target policy | Error if invalid | Tests |
|---|---|---|---|---|
| Time window | Stubbed | Map to visibility query | `INVALID_ARGUMENT` for invalid range | Equivalence |
| Workflow id | Stubbed | Map to visibility query | n/a | Equivalence |
| Workflow type | Stubbed | Map to visibility query | n/a | Equivalence |
| Execution status | Stubbed | Enforce open/closed partition | n/a | Status partition |
| Archived listing | Stubbed | Map to modern visibility query with archived/closed semantics documented | `INVALID_ARGUMENT` for invalid filter | Equivalence |
| Scan query | Stubbed | Map to modern query/list-equivalent behavior | `INVALID_ARGUMENT` for invalid query | Equivalence |
| Search attributes catalog | Stubbed | Return system and stored custom attributes | n/a | Catalog |

## Requirements

### Requirement 1: Open and Closed Listing

**User Story:** As an SDK/tooling user, I want legacy open/closed list RPCs to return matching executions, so that older clients continue to work.

#### Acceptance Criteria

1. `ListOpenWorkflowExecutions` SHALL return only open executions in the requested namespace and time window.
2. `ListClosedWorkflowExecutions` SHALL return only closed executions in the requested namespace and time window.
3. Supported filters by workflow id, type, and status SHALL map to the visibility query model.
4. Invalid filter variants SHALL return `INVALID_ARGUMENT` before querying.
5. Pagination tokens SHALL be stable and scoped to the request.

### Requirement 2: Archived and Scan Semantics

**User Story:** As an operator, I want archive and scan behavior to be explicit, so that legacy clients receive predictable projection-backed results.

#### Acceptance Criteria

1. `ListArchivedWorkflowExecutions` SHALL translate to the modern visibility query path and return archived-compatible results from the current visibility store.
2. IF scan semantics are equivalent to list in the current store, `ScanWorkflowExecutions` SHALL document and implement that behavior.
3. Scan SHALL honor namespace, query, page size, and page token.

### Requirement 3: Search Attributes Catalog

**User Story:** As a client, I want `GetSearchAttributes` to return indexed attributes, so that query builders can discover supported fields.

#### Acceptance Criteria

1. `GetSearchAttributes` SHALL return all supported system and custom search attributes known to the visibility layer.
2. Unsupported attribute types SHALL not be advertised.
3. If custom search attributes are not stored, THE RPC SHALL return system attributes and an empty custom set.
