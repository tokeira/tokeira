# Implementation Plan: Temporal Compatibility Surface

## Overview

This plan adds a queryable coverage API to the existing `tokeira-compatibility` crate. The work is
purely additive: a new `coverage` module with serializable result types, an expected-outcome
projection over the existing `dispatch_rpc` policy, a runtime RPC→feature index, vendored-proto-derived
wire-path normalization, and a single total `resolve` entry point composing them. No new crate, no new
third-party dependency, no I/O, no async.

The module is split into submodules (`normalize`, `index`, `expected`, plus `mod.rs` for the shared
result types and `resolve`) so that independent pieces can be built and tested in parallel without
touching the same file. All tests are co-located under `#[cfg(test)]`, consistent with the crate, and
`proptest` is already a dev-dependency. Property tests are tagged
`// Feature: temporal-compatibility-surface, Property N`.

## Tasks

- [x] 1. Establish the coverage module and serializable result types
  - [x] 1.1 Create the `coverage` module skeleton, result types, and crate wiring
    - Create `crates/tokeira-compatibility/src/coverage/mod.rs` defining `ExpectedOutcome` (variants
      `Ok`, `OkWhenEnabled`, `DisabledPrecondition`, `Unimplemented`) and `RpcClassification`
      (`Known { rpc_id, feature_id, state, expected }` and `UnknownToMatrix { wire_path }`)
    - Derive `Debug, Clone, PartialEq, Eq, Serialize, Deserialize` on both types so consumers can embed
      them in a coverage report
    - Declare the submodules `pub mod normalize; pub mod index; pub mod expected;` and create minimal
      compiling stub files for each (`crates/tokeira-compatibility/src/coverage/{normalize,index,expected}.rs`)
    - Add `pub mod coverage;` and re-export the result types from
      `crates/tokeira-compatibility/src/lib.rs`
    - Full WHY-not-WHAT doc comments on the module and every public item
    - _Requirements: 3.1, 4.3, 5.1, 5.2, 5.3, 5.4_

- [x] 2. Implement the expected-outcome projection
  - [x] 2.1 Implement `expected_outcome`
    - In `crates/tokeira-compatibility/src/coverage/expected.rs`, implement
      `expected_outcome(entry: &FeatureEntry, dynamic_config: &dyn DynamicConfigReader, namespace: Option<&str>) -> ExpectedOutcome`
      as a thin projection over the existing `dispatch.rs` policy (single policy, two views)
    - Map `Implemented | Partial → Ok`; `Experimental` gated ON → `OkWhenEnabled`, gated OFF →
      `DisabledPrecondition`; `Stubbed | Unsupported → Unimplemented` — mirroring `DispatchOutcome`
    - Document the `Partial`-as-`Implemented`-equivalent verdict decision inline
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 6.1, 6.2_

  - [x]* 2.2 Write property test for expected-outcome agreement
    - **Property 3: Expected-outcome agrees with dispatch policy**
    - For every `FeatureState` (and, for `Experimental`, both dynamic-config states), assert
      `expected_outcome` is consistent with `dispatch_rpc`'s `DispatchOutcome` for the same inputs
    - Tag `// Feature: temporal-compatibility-surface, Property 3`
    - **Validates: Requirements 3.1, 3.2**

- [x] 3. Implement the runtime RPC→feature index
  - [x] 3.1 Implement `feature_for_rpc`
    - In `crates/tokeira-compatibility/src/coverage/index.rs`, implement
      `feature_for_rpc(rpc_id: &str) -> Option<&'static FeatureEntry>`, built by scanning the existing
      `FEATURE_MATRIX` (promote today's test-only RPC set to real API)
    - Return `None` for an rpc id the matrix does not classify; no `.unwrap()` outside tests
    - _Requirements: 2.1, 2.2, 2.3_

  - [x]* 3.2 Write property test for index totality and uniqueness
    - **Property 2: Index totality and uniqueness**
    - Assert `feature_for_rpc` resolves every classified `rpc_id` in `FEATURE_MATRIX` to exactly one
      entry, and returns `None` for an unclassified id
    - Tag `// Feature: temporal-compatibility-surface, Property 2`
    - **Validates: Requirements 2.1, 2.2**

- [x] 4. Implement vendored-proto-derived wire-path normalization
  - [x] 4.1 Implement wire-path ↔ rpc-id mapping
    - In `crates/tokeira-compatibility/src/coverage/normalize.rs`, implement
      `wire_path_to_rpc_id(wire_path: &str) -> Option<String>` mapping
      `/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution` →
      `WorkflowService.StartWorkflowExecution`, and the inverse
      `rpc_id_to_wire_path(rpc_id: &str) -> Option<String>`
    - Derive the service/package mapping from the vendored protos under `proto/upstream/` (reuse the
      `include_str!` + line-parsing approach from `matrix_classifies_every_upstream_rpc` in
      `matrix.rs`); never read generated artifacts under `target/`
    - Return a not-recognized result (not a panic) when a path does not parse as `/package.Service/Method`
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [x]* 4.2 Write property test for normalization round-trip
    - **Property 1: Normalization round-trip**
    - For every `rpc_id` in `FEATURE_MATRIX`, assert `wire_path_to_rpc_id(rpc_id_to_wire_path(rpc_id)) == rpc_id`
      and that the wire path matches the vendored descriptor set's method path for that RPC
    - Tag `// Feature: temporal-compatibility-surface, Property 1`
    - **Validates: Requirements 1.1, 1.2**

- [x] 5. Compose the public `resolve` entry point
  - [x] 5.1 Implement `resolve`
    - In `crates/tokeira-compatibility/src/coverage/mod.rs`, implement
      `resolve(wire_path: &str, dynamic_config: &dyn DynamicConfigReader, namespace: Option<&str>) -> RpcClassification`
      composing normalization, `feature_for_rpc`, and `expected_outcome`
    - Return `Known { rpc_id, feature_id, state, expected }` for matrix-owned RPCs and
      `UnknownToMatrix { wire_path }` for everything else (Admin/Health/unparseable); total, never panics
    - Re-export `resolve`, `feature_for_rpc`, `expected_outcome`, and the normalization functions from
      `lib.rs`
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [x]* 5.2 Write property test for resolve totality
    - **Property 4: No observed RPC is dropped**
    - Assert `resolve` returns a `Known` classification for matrix-owned RPCs and `UnknownToMatrix`
      otherwise, for arbitrary input wire paths, and never panics
    - Tag `// Feature: temporal-compatibility-surface, Property 4`
    - **Validates: Requirements 4.1, 4.2**

- [x] 6. Checkpoint - verify the crate builds, lints, and is formatted
  - Run `cargo test -p tokeira-compatibility`, `cargo lint`, and `cargo +nightly fmt --all --check`
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional property-test sub-tasks and can be skipped for a faster MVP.
- Each task references specific requirements clauses for traceability; each property sub-task references
  its design Property number.
- The module is split into `normalize`/`index`/`expected`/`mod.rs` so independent implementation tasks
  never edit the same file, enabling parallel execution.
- All additions are projections over the existing `FEATURE_MATRIX` const and vendored protos — no new
  crate, no new third-party dependency, no I/O, no async (Requirement 5).

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "3.1", "4.1"] },
    { "id": 2, "tasks": ["2.2", "3.2", "4.2", "5.1"] },
    { "id": 3, "tasks": ["5.2"] }
  ]
}
```
