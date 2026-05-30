# Requirements Document

## Introduction

This spec implements OperatorService remote cluster RPCs currently stubbed: `AddOrUpdateRemoteCluster`, `RemoveRemoteCluster`, and `ListClusters`.

## Glossary

- **Remote cluster:** Operator-managed metadata for another Temporal-compatible cluster.
- **Cluster registry:** Durable store of local and remote cluster records.
- **Failover version:** Upstream cluster versioning metadata where supported.

## Target State

`Implemented`. The OperatorService remote cluster RPCs provide durable registry
CRUD for remote cluster metadata. This spec does not implement replication,
failover, remote task routing, or cross-cluster history transfer.

## Evidence From Current Code

- Proto messages inspected: `AddOrUpdateRemoteClusterRequest`, `RemoveRemoteClusterRequest`, `ListClustersRequest`.
- Current handlers: OperatorService remote cluster methods are stubbed.
- Existing implementation: no durable remote cluster registry, multi-cluster replication, namespace failover, or remote history routing model.

## Non-Goals

This spec does not implement replication, namespace failover, remote task
routing, history replication, remote membership, TLS credential provisioning, or
cross-cluster consistency. It implements only the registry metadata surface that
the OperatorService exposes.

## Remote Cluster Field Policy

| RPC/field group | Target policy | Status |
|---|---|---|
| `AddOrUpdateRemoteCluster` mutation | Upsert durable registry metadata | Implemented |
| `RemoveRemoteCluster` mutation | Remove durable remote registry metadata | Implemented |
| `ListClusters` local metadata | Return local cluster plus registered remote clusters | Implemented |
| Endpoint/address/access fields | Persist as registry metadata only | Implemented |
| Replication/failover behavior | Not activated by registry CRUD | Non-goal |

## Requirements

### Requirement 1: Remote Cluster Registry

**User Story:** As an operator, I want to add, update, remove, and list remote clusters, so that cluster metadata is visible through the Temporal OperatorService.

#### Acceptance Criteria

1. `AddOrUpdateRemoteCluster` SHALL persist cluster metadata including name, address, and `enable_remote_cluster_access`.
2. `AddOrUpdateRemoteCluster` SHALL update an existing remote cluster record atomically when the name already exists.
3. `RemoveRemoteCluster` SHALL remove a registered remote cluster record.
4. `ListClusters` SHALL return local cluster metadata plus all registered remote cluster records.
5. Pagination in `ListClusters` SHALL be stable and scoped to the request.
6. Registry CRUD SHALL NOT imply replication, namespace failover, or cross-cluster routing is active.

### Requirement 2: Validation and Error Behavior

**User Story:** As an operator, I want invalid cluster metadata rejected, so that cluster registry reads remain trustworthy.

#### Acceptance Criteria

1. Missing or empty cluster names SHALL return `INVALID_ARGUMENT`.
2. Removing an unknown remote cluster SHALL return `NOT_FOUND`.
3. Invalid address or access metadata SHALL return `INVALID_ARGUMENT`.
4. The local cluster SHALL NOT be removable through `RemoveRemoteCluster`.
5. Registry entries SHALL survive process restart for both memory-backed tests and DSQL-backed deployments.

### Requirement 3: Process Boundary

**User Story:** As a runtime maintainer, I want cluster metadata isolated from workflow correctness, so that remote cluster administration cannot corrupt per-run history.

#### Acceptance Criteria

1. Cluster registry mutations SHALL NOT write workflow history.
2. The registry SHALL live behind an operator/runtime-neutral trait boundary.
3. OperatorService handlers SHALL not depend on concrete runtime internals.
