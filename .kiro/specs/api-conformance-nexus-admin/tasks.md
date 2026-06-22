# Implementation Plan: Nexus Admin API Conformance

## Overview

Add a durable Nexus endpoint registry and implement the five OperatorService CRUD/list RPCs with
v1.31.0-faithful validation, error codes, and runtime dispatch integration. Every behaviour cites
`v1.31.0` (see `requirements.md` / `design.md`). Scope is admin CRUD only (clears
`TestNexusEndpointsFunctionalSuite` + `TestNexusAPIValidationTestSuite` ≈ 17 tests; not the ~80
operation-execution tests).

## Status — IMPLEMENTED (reconciled 2026-06-22)

The implementation landed but the checkboxes below were never reconciled. Verified against the
current tree on 2026-06-22 (code read end-to-end + `cargo test -p tokeira-edge --lib nexus_endpoint`
→ **13/13 pass**). Loci:

- **Store + live registry** — `crates/tokeira-runtime/src/nexus.rs`: `NexusEndpointStore` trait +
  `InMemoryNexusEndpointStore` (server-authored UUID id, version-1-on-create, monotonic CAS,
  unique-name index, id-ordered pagination, `find_by_name`/`resolve_by_name`); `NexusEndpointRegistry`
  is store-backed with an **owned** `resolve` (the old static `Arc<HashMap>` is gone).
- **Admin/validation/translation** — `crates/tokeira-edge/src/nexus_endpoint.rs`: `NexusEndpointAdmin`,
  `validate_upsert_spec` (verbatim v1.31.0 issue messages), API↔store translation, and
  store-error→gRPC mapping (`map_store_error`: version mismatch → `FAILED_PRECONDITION`, never
  `ABORTED`; duplicate → `ALREADY_EXISTS`; missing id → `NOT_FOUND`).
- **RPC handlers** — `crates/tokeira-edge/src/grpc/operator_service.rs`: all five Nexus endpoint RPCs
  delegate to the admin (no `UNIMPLEMENTED` stubs).
- **Config knobs** — `crates/tokeira-config/src/lib.rs`: `NexusEndpointLimitsConfig` (6 knobs +
  v1.31.0 defaults).
- **Shared-store wiring** — `apps/tokeirad/src/lib.rs`: one `nexus_store` →
  `NexusEndpointRegistry::new(store.clone())` for dispatch **and** `NexusEndpointAdmin::new(store.clone(), …)`
  for CRUD, then `new_with_nexus_and_shards_and_endpoint(… nexus_registry …)`. A runtime
  `CreateNexusEndpoint` is immediately resolvable for dispatch (Odori's per-session worker endpoint).

Remaining: the dedicated property tests P1–P4 were **not** authored as `proptest` blocks — the
behaviour is covered by 13 example-based unit tests (`nexus_endpoint::tests::*`) plus the store/registry
tests in `nexus.rs`/`lib.rs`. The operator conformance measurement of `TestNexusEndpointsFunctionalSuite`
(15) + `TestNexusAPIValidationTestSuite` (2) has not been run to confirm the ≈17-test denominator.

## Tasks

- [x] 1. Configuration surface (raise, never hardcode — mandate rule 3)
  - [x] 1.1 Model the six limit knobs with v1.31.0 defaults
    - name max 200, external URL max 4096, description max 20000, task-queue max 1000, list default
      page size 100, list max page size 1000 (sources in `requirements.md` Configuration Surface).
    - _Requirements: 2.1, 2.2, 2.3, 2.8_

- [x] 2. Endpoint store model
  - [x] 2.1 Define `NexusEndpointStore` trait + in-memory implementation
    - Server-authored UUID id, unique-name index, monotonic version, `(name, id)` list ordering.
    - Place the trait at a neutral boundary to avoid an edge↔runtime cycle.
    - _Requirements: 1.1, 1.2, 1.5, 3.3_
  - [x] 2.2 Version/conflict CAS on update/delete
    - Mismatch → `FAILED_PRECONDITION` "nexus endpoint version mismatch…" (NOT `ABORTED`).
    - Duplicate name → `ALREADY_EXISTS`; missing id → `NOT_FOUND`.
    - _Requirements: 1.3, 1.4, 2.5, 2.6, 2.7_

- [x] 3. Translation + validation
  - [x] 3.1 Free translation functions for the OperatorService Nexus messages
    - Mirror `apiSpecToPersistenceSpec` / `apiTargetToPersistenceTarget` /
      `endpointPersistedEntryToExternalAPI @ v1.31.0`.
    - _Requirements: 1.1, 1.2, 1.5_
  - [x] 3.2 `validate_upsert_spec` (accumulating `RequestIssues` → one `INVALID_ARGUMENT`)
    - Name (non-empty / length / regex), target variant (Worker namespace set+exists [namespace
      missing → immediate `FAILED_PRECONDITION`] + task queue; External URL non-empty/length/parse/
      scheme), description size. Verbatim messages per the Error Handling table.
    - _Requirements: 2.1, 2.2, 2.3_
  - [x] 3.3 Id/delete validators
    - Id non-empty + UUID-parseable; delete version `> 0`.
    - _Requirements: 2.4_

- [x] 4. Handlers and runtime integration
  - [x] 4.1 Wire get/create/update/delete/list in `crates/tokeira-edge/src/grpc/operator_service.rs`
    - List: name-filter path ignores page args and returns 0-or-1; otherwise page-size bounds.
    - Reads return current committed state (read-after-write).
    - _Requirements: 1.1-1.5, 2.1-2.8_
  - [x] 4.2 Make `NexusEndpointRegistry` live-backed by the store (prerequisite for 4.3)
    - Replace the static `Arc<HashMap>` (`crates/tokeira-runtime/src/nexus.rs:72-86`) with a
      `NexusEndpointStore`-backed lookup; change `resolve` to return an **owned**
      `NexusEndpointConfig`; update the `publisher.rs` call site.
    - Wire the store into `TokeiraRuntime::new` in place of `NexusEndpointRegistry::default()`.
    - _Requirements: 3.1, 3.4, 3.5_
  - [x] 4.3 Confirm runtime dispatch resolves via the live registry
    - Created endpoints resolve; deleted endpoints do not.
    - _Requirements: 3.2, 3.3_

- [-] 5. Tests
  - [-] 5.1 Property: CRUD Round Trip (P1)
    - Covered example-based (`create_round_trips_with_authored_id_version_and_url_prefix`,
      `list_by_name_returns_single_match`); dedicated proptest not authored.
    - _Requirements: 1.1, 1.2, 1.5_
  - [-] 5.2 Property: Optimistic Update Safety (P2)
    - Covered example-based (`update_version_mismatch_is_failed_precondition`); dedicated proptest not authored.
    - _Requirements: 1.3, 1.4, 2.7_
  - [-] 5.3 Property: Validation Totality & Code Fidelity (P3)
    - Covered example-based (the name/target/url/id/delete-version validation tests assert exact code +
      message); dedicated proptest not authored.
    - _Requirements: 2.1-2.8_
  - [-] 5.4 Property: Runtime Visibility (P4)
    - Covered example-based (`build_nexus_endpoint_store_resolves_worker_namespace_names` resolves a
      created endpoint via the live registry); a created/deleted proptest pair not authored.
    - _Requirements: 3.2, 3.3_

- [x] 6. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test -p tokeira-edge`. (nexus_endpoint suite: 13/13 pass, 2026-06-22.)
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
