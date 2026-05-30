# Implementation Plan: Namespace Full Lifecycle Conformance

## Overview

Implement update, deprecate, and delete namespace RPCs with explicit lifecycle state and safe deletion checks.

## Tasks

- [ ] 1. Extend namespace data model
  - [ ] 1.1 Update namespace store/cache types in `crates/tokeira-edge/src/namespace_cache.rs`
    - Add lifecycle state and update/delete methods.
    - _Requirements: 1.1, 2.1, 3.1, 3.4_
  - [ ] 1.2 Add translation DTOs/functions
    - Use free functions in `crates/tokeira-edge/src/grpc/translate.rs`.
    - _Requirements: 1.1, 1.4, 3.1_

- [ ] 2. Implement handlers
  - [ ] 2.1 Implement `update_namespace`
    - Store namespace config fields; reject multi-cluster/global namespace config with `INVALID_ARGUMENT`.
    - _Requirements: 1.1, 1.2, 1.3, 1.4_
  - [ ] 2.2 Implement `deprecate_namespace`
    - Mark deprecated idempotently.
    - _Requirements: 2.1, 2.4_
  - [ ] 2.3 Implement OperatorService `delete_namespace`
    - Check open executions before delete/tombstone.
    - _Requirements: 3.1, 3.2, 3.3, 3.5_

- [ ] 3. Enforce lifecycle behavior
  - [ ] 3.1 Reject new starts in deprecated namespaces
    - Update start and signal-with-start paths.
    - _Requirements: 2.2_
  - [ ] 3.2 Preserve read behavior for deprecated namespaces
    - _Requirements: 2.3_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: Namespace Lifecycle Monotonicity
    - _Requirements: 2.1, 3.4_
  - [ ] 4.2 Property test: Deprecated Read/Write Split
    - _Requirements: 2.2, 2.3_
  - [ ] 4.3 Property test: Safe Delete
    - _Requirements: 3.1, 3.2, 3.5_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2", "2.3"] },
    { "id": 2, "tasks": ["3.1", "3.2"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```
