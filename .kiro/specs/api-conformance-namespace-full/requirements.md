# Requirements Document

## Introduction

This spec implements namespace lifecycle RPCs currently stubbed: `UpdateNamespace`, `DeprecateNamespace`, and OperatorService `DeleteNamespace`.

## Glossary

- **Namespace record:** Edge/operator metadata for namespace configuration and lifecycle state.
- **Deprecated namespace:** A namespace that remains readable but should reject new workflow starts.
- **Deleted namespace:** A namespace removed or tombstoned according to retention rules.

## Target State

`Implemented`. Namespace metadata lifecycle is implemented for namespace
configuration fields. Tokeira remains single-cluster: multi-cluster
replication/global namespace configurations are invalid inputs and return
`INVALID_ARGUMENT`.

## Evidence From Current Code

- Proto messages inspected: `UpdateNamespaceRequest`, `DeprecateNamespaceRequest`, OperatorService `DeleteNamespaceRequest`.
- Current handlers: `update_namespace`, `deprecate_namespace`, `delete_namespace`.
- Existing store/cache: `NamespaceCache`, `ResolvedNamespace`, in-memory namespace registry.
- Admission paths needing enforcement: start, signal-with-start, schedules, batch, multi-operation.

## Namespace Field Policy

| Field group | Current state | Target policy | Error if invalid | Storage/cache impact |
|---|---|---|---|---|
| Description/owner/email/custom attributes | Partial | Update supported metadata | validation errors | Namespace record/cache |
| Retention/config fields | Partial | Store and return namespace config | validation errors | Namespace record |
| Global namespace / replication config | Not supported | Accept absent/local-only config; reject multi-cluster/global config | `INVALID_ARGUMENT` | Single-cluster namespace record |
| Deprecation state | Stubbed | Store lifecycle state | n/a | Admission guard |
| Delete/tombstone | Stubbed | Tombstone, do not delete histories | `FAILED_PRECONDITION` for open executions | Cache invalidation |

## Admission Enforcement Matrix

Deprecated namespaces must reject direct starts, signal-with-start start branch,
schedule firing, batch starts, and multi-operation starts through one shared
namespace admission guard.

## Requirements

### Requirement 1: UpdateNamespace

**User Story:** As an operator, I want to update namespace metadata and config, so that namespace behavior can change without recreation.

#### Acceptance Criteria

1. WHEN a namespace exists, THE RPC SHALL update supported config fields and return the updated namespace.
2. WHEN retention, archival, description, owner/email, custom attributes, or other namespace config fields are supplied, THE registry SHALL store them and `DescribeNamespace` SHALL return them.
3. WHEN the namespace does not exist, THE RPC SHALL return `NOT_FOUND`.
4. WHEN the request is invalid, THE RPC SHALL return `INVALID_ARGUMENT`.
5. WHEN `replication_config` is absent or contains only the local cluster, THE RPC SHALL accept it and store the namespace config.
6. WHEN `replication_config` specifies clusters beyond the local cluster, THE RPC SHALL return `INVALID_ARGUMENT` with a message that Tokeira operates in single-cluster mode.
7. WHEN `is_global_namespace` is true, THE RPC SHALL return `INVALID_ARGUMENT` because global namespaces require multi-cluster replication.

### Requirement 2: DeprecateNamespace

**User Story:** As an operator, I want to deprecate a namespace, so that new executions are blocked while existing data remains inspectable.

#### Acceptance Criteria

1. WHEN a namespace exists, THE RPC SHALL mark it deprecated.
2. WHEN a namespace is deprecated, new workflow starts in that namespace SHALL be rejected with `FAILED_PRECONDITION`.
3. Reads and visibility queries for deprecated namespaces SHALL continue to work.
4. Repeating deprecation SHALL be idempotent.

### Requirement 3: DeleteNamespace

**User Story:** As an operator, I want to delete a namespace through OperatorService, so that test and administrative namespaces can be removed safely.

#### Acceptance Criteria

1. WHEN the namespace exists and has no open executions, THE RPC SHALL delete or tombstone it.
2. WHEN open executions exist, THE RPC SHALL return `FAILED_PRECONDITION` unless force-delete semantics are explicitly implemented.
3. WHEN the namespace does not exist, THE RPC SHALL return `NOT_FOUND`.
4. Deleted namespaces SHALL not appear in normal `ListNamespaces` results unless an include-deleted option exists and is requested.
5. Deleting a namespace SHALL NOT delete run history unless a separate destructive retention path is implemented.
