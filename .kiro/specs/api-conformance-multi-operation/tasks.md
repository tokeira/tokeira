# Implementation Plan: Multi-Operation API Conformance

## Overview

Implement `ExecuteMultiOperation` for same-run operation groups using one runtime submission, one kernel transition, and one storage commit.

## Tasks

- [ ] 1. Add translation and validation
  - [ ] 1.1 Add multi-operation DTOs and free translation functions
    - Cover every upstream operation oneof variant.
    - Translate start, update, and signal-style supported operations.
    - Reject unknown variants with `INVALID_ARGUMENT`.
    - _Requirements: 1.1, 1.3, 3.1, 3.2_
  - [ ] 1.2 Validate single-run atomic scope
    - Resolve the common target workflow for the operation group.
    - Reject cross-run groups before mutation.
    - _Requirements: 2.1, 2.4_

- [ ] 2. Add runtime/kernel atomic path
  - [ ] 2.1 Add `TokeiraRuntime::execute_multi_operation`
    - Route same-run operation groups to one lane submit.
    - Return per-operation results in request order.
    - _Requirements: 1.2, 1.4, 2.3_
  - [ ] 2.2 Add kernel multi-operation application
    - Apply validated operations in one transition using existing command handlers where possible.
    - Emit history events in deterministic operation order.
    - _Requirements: 1.2, 2.2, 2.3_
  - [ ] 2.3 Commit through existing storage OCC/fencing once
    - Ensure injected failure leaves no partial state.
    - _Requirements: 2.1, 2.2_

- [ ] 3. Wire handler and errors
  - [ ] 3.1 Implement `execute_multi_operation` handler
    - Translate, validate, call runtime, and serialize ordered results.
    - _Requirements: 1.1-1.4, 2.1_
  - [ ] 3.2 Verify gRPC errors and metrics
    - _Requirements: 3.1, 3.2, 3.3_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: Validate Before Mutate
    - _Requirements: 1.2, 1.3, 2.1_
  - [ ] 4.2 Property test: Atomic Result Ordering
    - _Requirements: 1.4, 2.3_
  - [ ] 4.3 Property test: No Partial Commit
    - _Requirements: 2.1, 2.2, 2.3_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.

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
