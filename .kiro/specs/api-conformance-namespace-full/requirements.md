# Requirements Document

## Introduction

This spec implements the full single-cluster namespace lifecycle surface for the
Temporal v1.31.0 compatibility target: `UpdateNamespace`, `DeprecateNamespace`,
OperatorService `DeleteNamespace`, and namespace admission guards. Wire shape comes
from the vendored Temporal API; observable behaviour comes from Temporal server
v1.31.0. Multi-cluster replication and failover remain outside Tokeira's
single-cluster product scope.

## Glossary

- **Namespace record:** Metadata that identifies a namespace and stores its lifecycle
  state and configuration.
- **Namespace ID:** Stable UUID identity of a namespace. Renaming a namespace does not
  change this identity.
- **Deleted-name tombstone:** The temporary renamed namespace record, in state
  `DELETED`, used while namespace-owned executions are reclaimed.
- **Reclaim:** Authoritative deletion of every workflow execution owned by a namespace,
  followed by removal of the namespace record.
- **Delete delay:** Time between successful execution reclaim and final namespace-record
  removal. An explicit request value takes precedence over the cluster default.
- **Task-token namespace:** Namespace identity embedded in, or authoritatively resolved
  from, a worker task token.

## Target State

`Implemented`. Namespace metadata lifecycle and OperatorService deletion match the
single-cluster behaviour of Temporal v1.31.0. Deletion accepts namespaces containing
open or closed executions: it marks and renames the namespace synchronously, starts
reclaim asynchronously, and removes the tombstone after reclaim and the selected delete
delay. The v1.31.0 default delete delay is zero. Tokeira does not expose Temporal's
dynamic-config system; its default protected-namespace list is empty, while its system
namespace remains undeletable.

## Evidence From Current Code

- **Contract shape (authoritative):**
  `proto/upstream/temporal/api/operatorservice/v1/request_response.proto`,
  `DeleteNamespaceRequest` and `DeleteNamespaceResponse`.
- **Request validation and task-token namespace behaviour:**
  `common/rpc/interceptor/namespace_validator.go @ v1.31.0`.
- **Delete orchestration and response timing:**
  `service/frontend/operator_handler.go`,
  `service/worker/deletenamespace/workflow.go`, and
  `service/worker/deletenamespace/reclaimresources/workflow.go @ v1.31.0`.
- **Mark, rename, collision handling, and protection:**
  `service/worker/deletenamespace/activities.go @ v1.31.0`.
- **Delete-delay default:**
  `common/dynamicconfig/constants.go @ v1.31.0`
  (`frontend.deleteNamespaceNamespaceDeleteDelay = 0`).
- **Functional contract:** `tests/namespace_delete_test.go` and
  `tests/namespace_interceptor_test.go @ v1.31.0`.
- **Current Tokeira handlers:**
  `crates/tokeira-edge/src/grpc/operator_service.rs`,
  `crates/tokeira-edge/src/workflow_service.rs`, and
  `crates/tokeira-edge/src/namespace_cache.rs`.
- **Authoritative run deletion:** `RunRepository::delete_run_for_bundle` and
  `WorkflowRuntimeApi::delete_workflow`.

## Field Policy

### `DeleteNamespaceRequest`

| Field | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `namespace` (1) | Select by name when non-empty and `namespace_id` is empty | `INVALID_ARGUMENT` if both selectors are empty or both are set; `NOT_FOUND` if unknown | Resolves the stable namespace ID and current name |
| `namespace_id` (2) | Select by stable ID when non-empty and `namespace` is empty | `INVALID_ARGUMENT` if both selectors are empty or both are set; `NOT_FOUND` if unknown | Resolves the current namespace name and stable ID |
| `namespace_delete_delay` (3) | Explicit value wins, including explicit zero; absence uses the pinned v1.31.0 default of zero | Malformed protobuf duration is rejected at translation | Delays final tombstone removal after execution reclaim; does not delay mark-and-rename |

### `DeleteNamespaceResponse`

| Field | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `deleted_namespace` (1) | Return the temporary deleted name generated from the original name plus `-deleted-` and a collision-free namespace-ID prefix | n/a | Names the tombstone visible while asynchronous reclaim runs |

## Requirements

### Requirement 1: UpdateNamespace

**User Story:** As an operator, I want to update namespace metadata and config, so that
namespace behaviour can change without recreation.

#### Acceptance Criteria

1. WHEN a namespace exists, THE Namespace Service SHALL update every supported field
   present in the request.
2. WHEN a supported namespace field is updated, THE Namespace Service SHALL return the
   updated value from subsequent `DescribeNamespace` calls.
3. IF the namespace does not exist, THEN THE Namespace Service SHALL return `NOT_FOUND`.
4. IF the request is structurally invalid, THEN THE Namespace Service SHALL return
   `INVALID_ARGUMENT`.
5. WHEN `replication_config` is absent or names only the local cluster, THE Namespace
   Service SHALL accept the request.
6. IF `replication_config` names another cluster, THEN THE Namespace Service SHALL return
   `INVALID_ARGUMENT`.
7. IF `is_global_namespace` is true, THEN THE Namespace Service SHALL return
   `INVALID_ARGUMENT`.

### Requirement 2: DeprecateNamespace

**User Story:** As an operator, I want to deprecate a namespace, so that new executions
are blocked while existing data remains inspectable.

#### Acceptance Criteria

1. WHEN an active namespace is deprecated, THE Namespace Service SHALL persist the
   `DEPRECATED` lifecycle state.
2. WHILE a namespace is deprecated, THE admission layer SHALL reject new workflow starts
   with `FAILED_PRECONDITION`.
3. WHILE a namespace is deprecated, THE admission layer SHALL continue to permit reads.
4. WHEN deprecation is repeated for an already-deprecated namespace, THE Namespace
   Service SHALL treat the request idempotently.

### Requirement 3: DeleteNamespace

**User Story:** As an operator, I want to delete a namespace and its owned executions,
so that administrative namespaces can be fully reclaimed.

#### Acceptance Criteria

1. IF both `namespace` and `namespace_id` are empty, THEN THE Operator Service SHALL
   return `INVALID_ARGUMENT`.
2. IF both `namespace` and `namespace_id` are set, THEN THE Operator Service SHALL return
   `INVALID_ARGUMENT` with message `Only one of namespace name or Id should be set on request.`
3. WHEN exactly one selector identifies an existing namespace, THE Operator Service
   SHALL resolve its stable ID and current name.
4. IF the selected namespace does not exist, THEN THE Operator Service SHALL return
   `NOT_FOUND`.
5. IF the selected namespace is Tokeira's system namespace, THEN THE Operator Service
   SHALL return `FAILED_PRECONDITION`.
6. WHEN deletion is admitted, THE Namespace Service SHALL mark the namespace `DELETED`
   before returning success.
7. WHEN deletion is admitted, THE Namespace Service SHALL atomically rename the namespace
   to a collision-free `<original>-deleted-<namespace-id-prefix>` name without changing
   its namespace ID.
8. WHEN mark-and-rename succeeds, THE Operator Service SHALL return the temporary name in
   `deleted_namespace` without waiting for execution reclaim.
9. WHILE the deleted-name tombstone exists, THE Namespace Service SHALL return state
   `DELETED` when described by stable namespace ID.
10. WHEN deletion targets a namespace containing open or closed executions, THE reclaim
    coordinator SHALL delete every owned execution through the authoritative run-deletion
    path.
11. WHEN reclaim deletes namespace-owned executions, THE reclaim coordinator SHALL leave
    executions in every other namespace unchanged.
12. WHEN `namespace_delete_delay` is present, THE reclaim coordinator SHALL use that
    explicit duration, including explicit zero.
13. WHEN `namespace_delete_delay` is absent, THE reclaim coordinator SHALL use the pinned
    v1.31.0 default of zero.
14. WHEN execution reclaim and the selected delete delay finish, THE Namespace Service
    SHALL remove the deleted-name tombstone.
15. WHEN the tombstone has been removed, THE Namespace Service SHALL return
    `NamespaceNotFound` for `DescribeNamespace` by the deleted namespace ID.
16. WHEN the tombstone has been removed, THE admission layer SHALL return
    `NamespaceNotFound` for workflow operations using the original namespace name.
17. WHILE normal `ListNamespaces` omits deleted namespaces, THE Namespace Service SHALL
    exclude deleted-name tombstones unless `include_deleted` is requested.

### Requirement 4: Task-Token Namespace Enforcement

**User Story:** As a namespace operator, I want worker task tokens fenced to their owning
namespace, so that a token cannot mutate a workflow through another namespace.

#### Acceptance Criteria

1. WHEN a worker response supplies a non-empty request namespace matching the task-token
   namespace, THE admission layer SHALL permit normal task processing.
2. IF a worker response supplies a non-empty request namespace different from the
   task-token namespace, THEN THE admission layer SHALL return `INVALID_ARGUMENT` with
   message `Operation requested with a token from a different namespace.`
3. IF namespace mismatch validation rejects a worker response, THEN THE admission layer
   SHALL leave workflow and edge-side response state unchanged.
4. WHEN a task-token request omits the request namespace, THE admission layer SHALL use
   the task-token namespace as the authoritative namespace.

## Iteration and Feedback Notes

- The previous deletion requirements incorrectly rejected namespaces with open
  executions and prohibited history deletion. Temporal v1.31.0 does the opposite:
  `DeleteNamespaceWorkflow` launches `ReclaimResourcesWorkflow`, which deletes owned
  executions before removing the namespace record. Tier 4.28 corrected all dependent
  requirements, design properties, and tasks to that verified contract.
