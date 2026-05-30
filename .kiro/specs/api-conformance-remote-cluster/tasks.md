# Implementation Plan: Remote Cluster API Conformance

## Overview

Implement remote cluster registry CRUD for OperatorService metadata without adding replication, failover, or cross-cluster routing semantics.

## Tasks

- [ ] 1. Add durable cluster registry
  - [ ] 1.1 Add `ClusterRegistry` trait
    - Support `upsert_remote_cluster`, `remove_remote_cluster`, and `list_clusters`.
    - Keep replication/failover behavior outside the trait.
    - _Requirements: 1.1-1.6, 3.2_
  - [ ] 1.2 Implement memory and DSQL registry stores
    - Persist name, address, `enable_remote_cluster_access`, version/update metadata, and local/remote marker.
    - Add restart/reload tests for durable records.
    - _Requirements: 1.1-1.6, 2.5_

- [ ] 2. Wire OperatorService handlers
  - [ ] 2.1 Implement `AddOrUpdateRemoteCluster`
    - Validate name/address/access metadata, upsert the registry record, and return the stored metadata.
    - _Requirements: 1.1, 1.2, 2.1, 2.3_
  - [ ] 2.2 Implement `RemoveRemoteCluster`
    - Reject local-cluster removal, remove remote records, and return `NOT_FOUND` for unknown records.
    - _Requirements: 1.3, 2.2, 2.4_
  - [ ] 2.3 Implement `ListClusters`
    - Return local cluster plus registered remote clusters with stable pagination.
    - _Requirements: 1.4, 1.5_
  - [ ] 2.4 Verify error mappings and metrics
    - _Requirements: 2.1-2.4_

- [ ] 3. Add required tests
  - [ ] 3.1 Property test: Registry CRUD Fidelity
    - _Requirements: 1.1-1.5_
  - [ ] 3.2 Property test: Local Cluster Protection
    - _Requirements: 2.4_
  - [ ] 3.3 Property test: Registry Isolation
    - _Requirements: 3.1, 3.3_
  - [ ] 3.4 Restart/recovery test: Durable Registry
    - _Requirements: 2.5_

- [ ] 4. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-storage`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2", "2.3", "2.4"] },
    { "id": 2, "tasks": ["3.1", "3.2", "3.3", "3.4"] },
    { "id": 3, "tasks": ["4"] }
  ]
}
```
