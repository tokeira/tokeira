# Implementation Plan: Schedule Field API Conformance

## Overview

Complete schedule transport field accounting and preserve existing schedule lifecycle behavior.

## Tasks

- [ ] 1. Classify schedule fields
  - [ ] 1.1 Add field accounting tests in `crates/tokeira-edge/src/translate/schedule.rs`
    - Cover fields listed in `UNSUPPORTED_FIELDS.md`.
    - _Requirements: 1.1, 1.2, 2.1, 2.2, 2.3_

- [ ] 2. Preserve schedule spec fields
  - [ ] 2.1 Preserve authored schedule representation where required
    - Extend `ScheduleState` only with serializable deterministic data.
    - _Requirements: 1.1, 1.2, 1.3_
  - [ ] 2.2 Ensure patch/update preserve unselected fields
    - _Requirements: 1.4_

- [ ] 3. Preserve schedule action metadata
  - [ ] 3.1 Apply start-field policy to `NewWorkflowExecutionInfo`
    - Headers, user metadata, and versioning override are stored and threaded into scheduled workflow starts.
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [ ] 4. Preserve lifecycle semantics
  - [ ] 4.1 Verify create/describe/update/patch/delete/list/count handlers
    - Keep existing not-found, duplicate, pagination, count, and matching-time semantics.
    - _Requirements: 3.1-3.5_

- [ ] 5. Add required tests
  - [ ] 5.1 Property test: Schedule Round Trip
    - _Requirements: 1.1, 1.2, 1.3_
  - [ ] 5.2 Property test: Metadata Firing Fidelity
    - _Requirements: 2.1, 2.2, 2.3, 2.4_
  - [ ] 5.3 Property test: Lifecycle Preservation
    - _Requirements: 3.1-3.5_

- [ ] 6. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "3.1"] },
    { "id": 2, "tasks": ["2.2", "4.1"] },
    { "id": 3, "tasks": ["5.1", "5.2", "5.3"] },
    { "id": 4, "tasks": ["6"] }
  ]
}
```
