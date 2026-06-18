# Implementation Plan: Nexus Admin API Conformance

## Overview

Add a durable Nexus endpoint registry and implement the five OperatorService CRUD/list RPCs with
v1.31.0-faithful validation, error codes, and runtime dispatch integration. Every behaviour cites
`v1.31.0` (see `requirements.md` / `design.md`). Scope is admin CRUD only (clears
`TestNexusEndpointsFunctionalSuite` + `TestNexusAPIValidationTestSuite` ≈ 17 tests; not the ~80
operation-execution tests).

## Tasks

- [ ] 1. Configuration surface (raise, never hardcode — mandate rule 3)
  - [ ] 1.1 Model the six limit knobs with v1.31.0 defaults
    - name max 200, external URL max 4096, description max 20000, task-queue max 1000, list default
      page size 100, list max page size 1000 (sources in `requirements.md` Configuration Surface).
    - _Requirements: 2.1, 2.2, 2.3, 2.8_

- [ ] 2. Endpoint store model
  - [ ] 2.1 Define `NexusEndpointStore` trait + in-memory implementation
    - Server-authored UUID id, unique-name index, monotonic version, `(name, id)` list ordering.
    - Place the trait at a neutral boundary to avoid an edge↔runtime cycle.
    - _Requirements: 1.1, 1.2, 1.5, 3.3_
  - [ ] 2.2 Version/conflict CAS on update/delete
    - Mismatch → `FAILED_PRECONDITION` "nexus endpoint version mismatch…" (NOT `ABORTED`).
    - Duplicate name → `ALREADY_EXISTS`; missing id → `NOT_FOUND`.
    - _Requirements: 1.3, 1.4, 2.5, 2.6, 2.7_

- [ ] 3. Translation + validation
  - [ ] 3.1 Free translation functions for the OperatorService Nexus messages
    - Mirror `apiSpecToPersistenceSpec` / `apiTargetToPersistenceTarget` /
      `endpointPersistedEntryToExternalAPI @ v1.31.0`.
    - _Requirements: 1.1, 1.2, 1.5_
  - [ ] 3.2 `validate_upsert_spec` (accumulating `RequestIssues` → one `INVALID_ARGUMENT`)
    - Name (non-empty / length / regex), target variant (Worker namespace set+exists [namespace
      missing → immediate `FAILED_PRECONDITION`] + task queue; External URL non-empty/length/parse/
      scheme), description size. Verbatim messages per the Error Handling table.
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 3.3 Id/delete validators
    - Id non-empty + UUID-parseable; delete version `> 0`.
    - _Requirements: 2.4_

- [ ] 4. Handlers and runtime integration
  - [ ] 4.1 Wire get/create/update/delete/list in `crates/tokeira-edge/src/grpc/operator_service.rs`
    - List: name-filter path ignores page args and returns 0-or-1; otherwise page-size bounds.
    - Reads return current committed state (read-after-write).
    - _Requirements: 1.1-1.5, 2.1-2.8_
  - [ ] 4.2 Make `NexusEndpointRegistry` live-backed by the store (prerequisite for 4.3)
    - Replace the static `Arc<HashMap>` (`crates/tokeira-runtime/src/nexus.rs:72-86`) with a
      `NexusEndpointStore`-backed lookup; change `resolve` to return an **owned**
      `NexusEndpointConfig`; update the `publisher.rs` call site.
    - Wire the store into `TokeiraRuntime::new` in place of `NexusEndpointRegistry::default()`.
    - _Requirements: 3.1, 3.4, 3.5_
  - [ ] 4.3 Confirm runtime dispatch resolves via the live registry
    - Created endpoints resolve; deleted endpoints do not.
    - _Requirements: 3.2, 3.3_

- [ ] 5. Tests
  - [ ] 5.1 Property: CRUD Round Trip (P1)
    - _Requirements: 1.1, 1.2, 1.5_
  - [ ] 5.2 Property: Optimistic Update Safety (P2)
    - _Requirements: 1.3, 1.4, 2.7_
  - [ ] 5.3 Property: Validation Totality & Code Fidelity (P3)
    - Assert exact gRPC code + message for each invalid-field class.
    - _Requirements: 2.1-2.8_
  - [ ] 5.4 Property: Runtime Visibility (P4)
    - Created endpoint resolves; deleted endpoint does not (live registry).
    - _Requirements: 3.2, 3.3_

- [ ] 6. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["3.1", "3.2", "3.3"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5.1", "5.2", "5.3", "5.4"] },
    { "id": 5, "tasks": ["6"] }
  ]
}
```

## Notes

- Scope is admin CRUD only (C4a). Operation execution / task transport (C4b) is owned by
  `edge-nexus-task-transport` / `kernel-nexus-operations` / `runtime-nexus-dispatch`.
- Every error code/message is pinned to `v1.31.0` in `design.md`'s Error Handling table — assert
  the exact code **and** message in tests. Notably: version mismatch is `FAILED_PRECONDITION` (never
  `ABORTED`); there is no `UNIMPLEMENTED`-on-unsupported-field path.
- The frontend/matching collapse is a deliberate internal-topology deviation; the observable contract
  must still match v1.31.0.
