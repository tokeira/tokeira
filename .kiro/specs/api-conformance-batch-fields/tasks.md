# Implementation Plan: Batch Field API Conformance

## Overview

Complete batch action field handling and batch response projection while preserving existing batch lifecycle semantics.

## Tasks

- [ ] 1. Account for batch action fields
  - [ ] 1.1 Update `crates/tokeira-edge/src/translate/batch.rs`
    - Thread signal headers, update-options payloads, and reset reapply fields into batch state.
    - _Requirements: 1.1, 1.2, 1.3, 1.4_
  - [ ] 1.2 Verify start handler preserves action fields before creating batch state
    - _Requirements: 3.1, 3.2_
  - [ ] 1.3 Wire batch dispatch for full action payloads
    - Signal actions pass headers to `SignalRequest`.
    - Update-options actions call the workflow-options runtime path for each target.
    - Reset actions pass reapply/current-run-only/exclude fields to the kernel reset command.
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 1.4 Add `BatchOperationUpdateActivityOptions` translation, durable action state, and dispatch
    - Preserve identity, type/match-all selector, options, update mask, and restore-original.
    - Delegate each workflow mutation to the single-workflow activity-options runtime path.
    - _Requirements: 1.5, 2.1, 3.2_

- [ ] 2. Complete lifecycle projection
  - [ ] 2.1 Persist supported request metadata in `BatchOperationStore`
    - _Requirements: 2.1, 2.2_
  - [ ] 2.2 Update describe/list projection
    - Stable pagination and progress summaries.
    - _Requirements: 2.2, 2.3_
  - [ ] 2.3 Preserve stop idempotence
    - _Requirements: 2.4_

- [ ] 3. Add required tests
  - [ ] 3.1 Property test: No Batch Field Drop
    - _Requirements: 1.1-1.4, 3.2_
  - [ ] 3.2 Property test: Progress Monotonicity
    - _Requirements: 2.2, 2.3_
  - [ ] 3.3 Property test: Stop Idempotence
    - _Requirements: 2.4_
  - [ ] 3.4 Property test: Property 4 — Batch activity-options equivalence
    - Use a reference model for at least 100 generated action/workflow/activity sets.
    - Tag: `// Feature: api-conformance-batch-fields, Property 4: batch activity-options equivalence`
    - _Requirements: 1.5, 2.1_

- [ ] 4. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.3", "1.4", "2.1"] },
    { "id": 2, "tasks": ["2.2", "2.3"] },
    { "id": 3, "tasks": ["3.1", "3.2", "3.3", "3.4"] },
    { "id": 4, "tasks": ["4"] }
  ]
}
```
