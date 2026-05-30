# Implementation Plan: Nexus Admin API Conformance

## Overview

Add a durable Nexus endpoint registry and implement OperatorService CRUD/list RPCs with runtime dispatch integration.

## Tasks

- [ ] 1. Add endpoint store model
  - [ ] 1.1 Define `NexusEndpointStore` trait and in-memory implementation
    - Place the trait at a neutral boundary to avoid crate cycles.
    - _Requirements: 1.1-1.5, 3.3_
  - [ ] 1.2 Add version/conflict token handling
    - _Requirements: 1.3, 1.4, 2.4_

- [ ] 2. Add translation and errors
  - [ ] 2.1 Add free translation functions for OperatorService Nexus messages
    - _Requirements: 1.1-1.5, 2.1_
  - [ ] 2.2 Add expected error variants/mappings if needed
    - Verify `errors.rs`, `grpc/errors.rs`, and metrics labels.
    - _Requirements: 2.2, 2.3, 2.4, 2.5_

- [ ] 3. Implement handlers
  - [ ] 3.1 Wire get/create/update/delete/list in `crates/tokeira-edge/src/grpc/operator_service.rs`
    - _Requirements: 1.1-1.5, 2.1-2.5_
  - [ ] 3.2 Wire runtime Nexus dispatch lookup to the store trait
    - _Requirements: 3.1, 3.2, 3.3_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: CRUD Round Trip
    - _Requirements: 1.1, 1.2, 1.5_
  - [ ] 4.2 Property test: Optimistic Update Safety
    - _Requirements: 1.3, 1.4, 2.4_
  - [ ] 4.3 Property test: Runtime Visibility
    - _Requirements: 3.1, 3.2_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["3.1", "3.2"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```
