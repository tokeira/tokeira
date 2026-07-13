# Implementation Plan: Namespace Full Lifecycle Conformance

- [ ] 1. Extend namespace metadata operations
  - [x] 1.1 Populate and preserve stable namespace IDs
    - Ensure registered/pre-seeded namespaces expose one stable ID that survives rename.
    - _Requirements: 3.3, 3.7, 3.9_
  - [x] 1.2 Add ID lookup, atomic mark-and-rename, removal, and deleted filtering
    - Implement collision-safe deleted-name generation from the namespace-ID prefix.
    - _Requirements: 3.6, 3.7, 3.9, 3.14, 3.15, 3.17_
  - [ ] 1.3 Complete supported namespace-configuration persistence and round-trip
    - Preserve partial-update masks and reject global/multi-cluster configuration.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_
  - [ ] 1.4 Implement the deprecated lifecycle state and shared start-admission guard
    - Keep reads available and make repeated deprecation idempotent.
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [ ] 2. Add authoritative namespace-run enumeration
  - [x] 2.1 Extend `RunRepository` with `list_runs_for_namespace`
    - Document that results come from authoritative mutable state and are sorted.
    - _Requirements: 3.10, 3.11_
  - [ ] 2.2 Implement in-memory and DSQL repository queries
    - Add focused storage tests for completeness, isolation, and deterministic order.
    - _Requirements: 3.10, 3.11_

- [x] 3. Implement namespace reclaim orchestration
  - [x] 3.1 Add `NamespaceDeletionApi` and production wiring
    - Reuse runtime `delete_workflow` and visibility `apply_deletion`; do not add kernel
      commands or bypass fenced per-run deletion.
    - _Requirements: 3.10, 3.11, 3.16_
  - [x] 3.2 Add asynchronous reclaim, delay, and final tombstone removal
    - Retain the tombstone on failure; explicit delay presence wins over the zero default.
    - _Requirements: 3.8, 3.9, 3.12, 3.13, 3.14, 3.15_

- [x] 4. Implement the OperatorService wire path
  - [x] 4.1 Translate `DeleteNamespaceRequest` without losing selector or duration presence
    - _Requirements: 3.1, 3.2, 3.3, 3.12, 3.13_
  - [x] 4.2 Implement validation, mark-and-rename, response, and system protection
    - Preserve the exact v1.31.0 both-selector error message.
    - _Requirements: 3.1, 3.2, 3.4, 3.5, 3.6, 3.7, 3.8_
  - [x] 4.3 Make Describe-by-ID tombstone-aware and normal List deleted-filtered
    - _Requirements: 3.9, 3.15, 3.17_

- [x] 5. Implement task-token namespace enforcement
  - [x] 5.1 Compare the request namespace with the authoritative token/run namespace
    - Reject before any pending-query or runtime side effect; permit an omitted request
      namespace to defer to the token.
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

- [x] 6. Checkpoint: focused crates compile and example tests pass
  - Run `cargo +nightly fmt --all --check`.
  - Run focused `tokeira-storage` and `tokeira-edge` checks/tests.

- [ ] 7. Add required property tests
  - [x] 7.1 Property 1 — selector validation is mutation-free
    - Reference-model PBT, at least 100 cases.
    - Tag: `// Feature: api-conformance-namespace-full, Property 1: selector validation is mutation-free`
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  - [x] 7.2 Property 2 — mark-and-rename preserves identity
    - Reference-model PBT over occupied-name sets, at least 100 cases.
    - Tag: `// Feature: api-conformance-namespace-full, Property 2: mark-and-rename preserves identity`
    - _Requirements: 3.6, 3.7, 3.8, 3.9_
  - [x] 7.3 Property 3 — namespace reclaim is complete and isolated
    - Reference-model PBT over namespace-partitioned run sets, at least 100 cases.
    - Tag: `// Feature: api-conformance-namespace-full, Property 3: namespace reclaim is complete and isolated`
    - _Requirements: 3.10, 3.11, 3.16_
  - [x] 7.4 Property 4 — delete-delay precedence controls final removal
    - Use Tokio's paused clock; no explicit test sleeps; at least 100 cases.
    - Tag: `// Feature: api-conformance-namespace-full, Property 4: delete-delay precedence controls final removal`
    - _Requirements: 3.9, 3.12, 3.13, 3.14, 3.15, 3.17_
  - [x] 7.5 Property 5 — task-token namespace mismatch is side-effect free
    - Reference-model PBT over request/token namespace pairs, at least 100 cases.
    - Tag: `// Feature: api-conformance-namespace-full, Property 5: task-token namespace mismatch is side-effect free`
    - _Requirements: 4.1, 4.2, 4.3, 4.4_
  - [ ] 7.6 Property 6 — namespace configuration updates round-trip
    - Reference-model PBT over valid/invalid partial config updates, at least 100 cases.
    - Tag: `// Feature: api-conformance-namespace-full, Property 6: namespace configuration updates round-trip`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_
  - [ ] 7.7 Property 7 — deprecated namespace read/write split
    - Reference-model PBT over lifecycle transitions and admission/read operations, at
      least 100 cases.
    - Tag: `// Feature: api-conformance-namespace-full, Property 7: deprecated namespace read/write split`
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [ ] 8. Add wire and functional regression coverage
  - [ ] 8.1 Add gRPC tests for deletion by name/ID, exact validation errors, tombstone
    describe, open-workflow reclaim, and eventual removal
    - _Requirements: 3.1, 3.2, 3.3, 3.6, 3.8, 3.9, 3.10, 3.14, 3.15, 3.16_
  - [x] 8.2 Add the task-token mismatch/retry wire regression
    - _Requirements: 4.1, 4.2, 4.3_
  - [x] 8.3 Classify only Shape-2-internal namespace corpus leaves in the skip registry
    - Keep public namespace deletion and namespace-interceptor leaves required-pass.
    - _Requirements: 3.1, 3.10, 4.2_
  - [x] 8.4 Run both Tier 4.28 suites clean twice and record the conformance ledger row
    - _Requirements: 3.1–3.17, 4.1–4.4_

- [x] 9. Final checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run focused crate checks and tests for every touched crate.
  - Confirm the compatibility matrix no longer classifies `OperatorService.DeleteNamespace`
    as stubbed.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3", "1.4", "2.1"] },
    { "id": 1, "tasks": ["2.2", "5.1", "7.6", "7.7"] },
    { "id": 2, "tasks": ["3.1"] },
    { "id": 3, "tasks": ["3.2", "4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["6", "7.1", "7.2", "7.3", "7.4", "7.5"] },
    { "id": 5, "tasks": ["8.1", "8.2", "8.3"] },
    { "id": 6, "tasks": ["8.4", "9"] }
  ]
}
```

## Notes

- The prior safe-delete model was wrong for the v1.31.0 target: open executions do not
  cause `FAILED_PRECONDITION`; they are reclaimed asynchronously.
- Namespace deletion must reuse the authoritative per-run deletion path. Direct bulk SQL
  that bypasses run deletion tombstones, fencing, or visibility cleanup is not permitted.
- The kernel remains unchanged.
