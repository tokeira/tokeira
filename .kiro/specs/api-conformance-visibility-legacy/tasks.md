# Implementation Plan: Legacy Visibility API Conformance

## Overview

Implement legacy visibility RPCs by translating filters to the existing visibility API, including archived and scan compatibility wrappers.

## Tasks

- [ ] 1. Add legacy visibility DTOs and translators
  - [ ] 1.1 Add request DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Cover open, closed, archived, scan, and search attributes.
    - _Requirements: 1.1-1.5, 2.1-2.3, 3.1_
  - [ ] 1.2 Add free translation functions in `crates/tokeira-edge/src/grpc/translate.rs`
    - Validate filter variants and map invalid combinations to `INVALID_ARGUMENT`.
    - _Requirements: 1.3, 1.4, 2.3_

- [ ] 2. Extend visibility API where needed
  - [ ] 2.1 Add typed legacy filter support or query translation in `tokeira-projection`
    - _Requirements: 1.1, 1.2, 1.3, 2.3_
  - [ ] 2.2 Add search attribute catalog read path
    - _Requirements: 3.1, 3.2, 3.3_

- [ ] 3. Wire gRPC handlers
  - [ ] 3.1 Implement open/closed/scan handlers
    - _Requirements: 1.1-1.5, 2.2, 2.3_
  - [ ] 3.2 Implement archived handler as a modern visibility query wrapper
    - _Requirements: 2.1_
  - [ ] 3.3 Implement `GetSearchAttributes`
    - _Requirements: 3.1, 3.2, 3.3_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: Status Partition
    - _Requirements: 1.1, 1.2_
  - [ ] 4.2 Property test: Filter Equivalence
    - _Requirements: 1.3, 2.3_
  - [ ] 4.3 Property test: Archived Wrapper Equivalence
    - _Requirements: 2.1_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-projection`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["3.1", "3.2", "3.3"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```
