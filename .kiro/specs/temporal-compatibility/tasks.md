# Implementation Plan: Temporal Compatibility

## Overview

Embed compatibility metadata in Tokeira binaries, declare the feature and SDK matrices as single sources of truth, expose them via standard `GetSystemInfo` and a Tokeira-owned Buffa/connect-rust compatibility service, provide CLI inspection commands, and add Dagger-backed CI checks that prevent silent drift.

Target crates:

- `crates/tokeira-build-info/` — NEW library crate with compile-time metadata constants from a Dagger-generated manifest
- `crates/tokeira-compatibility/` — NEW library crate with `FEATURE_MATRIX`, `SDK_MATRIX`, `Feature` trait, `declare_feature!`, `cfg_feature!`, `dispatch_rpc`
- `crates/tokeira-compatibility-proto/` — NEW crate owning Buffa-generated messages and connect-rust service/client code
- `crates/tokeira-compatibility-service/` — NEW crate mapping matrices into Buffa DTOs, implementing connect-rust handlers
- `crates/tokeira-edge/` — extend with standard `GetSystemInfo` handler (upstream-only, no Tokeira extension fields) + `dispatch_rpc` adoption
- `crates/tokeira-kernel/` — adopt `cfg_feature!` at existing feature-gated module boundaries
- `apps/tkr/` — add `compat` command group (`show`, `diff`) and `ci` command group (`check`, `build`)
- `crates/tokeira-build/` — Dagger compatibility module, versioned build, lockfile policy

This plan does NOT include:
- `tkr compat bump` (deferred — manual governance only in MVP)
- GitHub API integration or automatic PR creation
- Extension fields on upstream `GetSystemInfoResponse`
- Remote CI wiring (Buildkite, GitHub Actions)
- Full SDK conformance orchestration

Correctness properties P1–P10 from the design are distributed across the tasks below.

## Tasks

- [ ] 1. Scaffold `crates/tokeira-build-info/`
  - [x] 1.1 Create the crate with zero runtime dependencies
    - Create `crates/tokeira-build-info/Cargo.toml` with `[build-dependencies]` only (`toml` for parsing `rust-toolchain.toml`, `regex` for parsing `pinned.rs`). The `[dependencies]` section is empty
    - Add `"crates/tokeira-build-info"` to `[workspace.members]` in the root `Cargo.toml`
    - In `crates/tokeira-build-info/src/lib.rs`, define the `BuildInfo` struct and the nine public constants (`TOKEIRA_VERSION`, `TOKEIRA_GIT_SHA`, `TEMPORAL_PROTO_VERSION`, `TEMPORAL_SERVER_COMPAT`, `RUST_TOOLCHAIN`, `SOURCE_TREE_HASH`, `FEATURE_MATRIX_DIGEST`, `SDK_MATRIX_DIGEST`, `BUILD_MODE`) each bound via `env!("TOKEIRA_BUILD_INFO_…")`
    - Add a `pub const fn summary() -> BuildInfo` returning the struct populated from the constants
    - _Requirements: 1.1–1.14_

  - [x] 1.2 Create `src/pinned.rs`
    - Declare `pub const TEMPORAL_PROTO_VERSION: &str = "v1.62.11";` and `pub const TEMPORAL_SERVER_COMPAT: &str = "1.31.0";` with doc comments citing the spec
    - These are the canonical version pins; bumping requires a spec update and passing matrix-completeness property tests
    - _Requirements: 33.1, 35.1, 35.2_

  - [x] 1.3 Implement `build.rs` with manifest-based metadata
    - Create `crates/tokeira-build-info/build.rs`
    - Emit `cargo:rerun-if-env-changed=TOKEIRA_BUILD_MANIFEST_PATH`
    - Emit `cargo:rerun-if-changed` for `../../rust-toolchain.toml`, `../../Cargo.toml`, `src/pinned.rs`
    - Implement `resolve_manifest_path()`: check `TOKEIRA_BUILD_MANIFEST_PATH` env var; if set, use that path (versioned mode)
    - Implement `is_dev_mode()`: returns true when `TOKEIRA_BUILD_MANIFEST_PATH` is not set
    - Implement `dev_fallback_manifest()`: derive `TOKEIRA_VERSION` from `CARGO_PKG_VERSION`; derive git SHA via `git rev-parse --short=8`; parse `src/pinned.rs` via regex for proto/server-compat pins; parse `../../rust-toolchain.toml` for toolchain; use placeholder zeros for source-tree hash; use `"dev"` for digests and build mode
    - Implement `parse_manifest(content: &str) -> Manifest`: simple key=value parser, one field per line, no quoting
    - Emit `cargo:rustc-env` for all nine constants
    - In versioned mode: panic with clear error if manifest is missing or malformed
    - **Do NOT** call `SystemTime::now`, `Utc::now`, `Local::now`, or any wall-clock source
    - _Requirements: 2.1–2.14, 3.1–3.12_

  - [x] 1.4 Implement version formatting in CLI layer
    - Create `apps/tkr/src/output/build_info.rs` for build-info output rendering
    - `pub fn format_version_short(info: &BuildInfo) -> String` — three-line summary
    - `pub fn format_version_verbose(info: &BuildInfo) -> String` — all fields
    - `pub fn format_version_json(info: &BuildInfo) -> String` — stable JSON with stable field names
    - The functions consume `BuildInfo` but live outside `tokeira-build-info`; the build-info crate remains metadata-only and owns no JSON, terminal, protobuf, YAML, or table rendering
    - All functions are pure and perform no I/O
    - _Requirements: 6.1–6.8_

  - [ ]* 1.5 Write property test: build metadata determinism (P9)
    - **Property P9: Build Metadata Determinism**
    - **Validates: Requirements 48**
    - Extract the manifest parsing and dev-fallback logic into pure helper functions testable without `build.rs` execution
    - Test: given the same input (manifest content or workspace state), `parse_manifest` produces identical output on two calls
    - Test: `dev_fallback_manifest` with mocked filesystem inputs produces identical output on two calls
    - Test: wall-clock patterns (`SystemTime::now`, `Utc::now`, `Local::now`) do not appear in `crates/tokeira-build-info/` source files (grep-based assertion)
    - Test framework: `proptest` for arbitrary manifest content round-trip
    - Test location: `crates/tokeira-build-info/tests/determinism.rs`

  - [ ] 1.6 Checkpoint — workspace compiles with new crate
    - Run `cargo +nightly fmt`, `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-build-info`
    - _Requirements: 1.1_

- [ ] 2. Scaffold `crates/tokeira-compatibility/`
  - [x] 2.1 Create the crate
    - Create `crates/tokeira-compatibility/Cargo.toml` with `thiserror`, `serde`, `tracing` as workspace-pinned deps, and `tokeira-build-info` as a path dep
    - Add `"crates/tokeira-compatibility"` to `[workspace.members]`
    - _Requirements: 15.1_

  - [x] 2.2 Define `FeatureState`, `CompatibilitySurfaceKind`, `CompatibilitySurface`, `CompatibilityEvidence`, `FeatureEntry`, `Feature` trait
    - In `src/feature.rs`: define `FeatureState { Implemented, Partial, Experimental, Stubbed, Unsupported }` with `Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq`
    - Define `CompatibilitySurfaceKind` enum with all nine variants per Requirement 14
    - Define `CompatibilitySurface { kind, identifier }` and `CompatibilityEvidence { kind, reference }`
    - Define `FeatureEntry` struct per design with `id`, `name`, `state`, `surfaces`, `capability_field`, `dynamic_config_key`, `rpcs`, `notes`, `evidence` (all `&'static` references)
    - Define `pub trait Feature { const ID: &'static str; const ENTRY: &'static FeatureEntry; }`
    - Define `pub const fn lookup_feature_const(id: &'static str) -> &'static FeatureEntry` with compile-time linear scan; `panic!` on miss
    - Define `pub const fn const_str_eq(a: &str, b: &str) -> bool` helper
    - _Requirements: 13.1–13.9, 14.1–14.12, 15.2–15.16_

  - [x] 2.3 Declare `FEATURE_MATRIX`
    - In `src/matrix.rs`, declare `pub const FEATURE_MATRIX: &[FeatureEntry]` with the initial seed
    - Every workflow-service and operator-service RPC maps to exactly one entry
    - Matrix MUST be sorted by feature ID; tests enforce this
    - Every `Experimental` entry SHALL have a non-None `dynamic_config_key`
    - Every entry's `rpcs` list SHALL be non-empty unless the feature is cross-cutting
    - Conservative initial states per Requirement 16
    - Seed states from `docs/temporal_api_audit.md`: strict-audit partial surfaces are marked `Partial`; unsupported upstream surfaces are marked `Unsupported`; future-ready surfaces are marked `Experimental`
    - Add `FeatureEntry::capability_fields()` so the current single `capability_field` proto/JSON shape remains stable while code can account for secondary mapped capabilities such as `workflow-task-lifecycle` → `upsert_memo`
    - _Requirements: 15.2–15.16, 16.1–16.13_

  - [x] 2.4 Implement `declare_feature!` and `cfg_feature!` macros
    - In `src/macros.rs`, implement both `macro_rules!` macros per the design
    - `declare_feature!($name:ident, $id:literal)` declares a zero-sized struct implementing `Feature`
    - `cfg_feature!($feature_id:literal => $($tt:tt)*)` emits a `const _: ()` block that panics at compile time if feature state is `Stubbed` or `Unsupported`, then emits the gated code
    - Export both macros with `#[macro_export]`
    - _Requirements: 15.1 (compile-time gates)_

  - [x] 2.5 Implement matrix digest (FNV-1a)
    - In `src/digest.rs`, implement FNV-1a hash over `(id, state_label, surface_identifiers, evidence_references)` tuples in declared order
    - For each entry, hash `id`, the `state` discriminant, each surface's `identifier`, and each evidence entry's `(kind, reference)` pair
    - Implement `pub fn feature_matrix_digest() -> String` and `pub fn sdk_matrix_digest() -> String`
    - The digest is computed at test time and compared against the manifest-embedded value; NOT computed at compile time via `const fn`
    - _Requirements: 49.1–49.6_

  - [x]* 2.6 Write property test: feature matrix digest stability (P6)
    - **Property P6: Feature Matrix Digest Stability**
    - **Validates: Requirements 49**
    - Compute `feature_matrix_digest()` twice; assert byte-equal
    - Assert that when a feature state changes, the digest changes
    - Assert that when a surface identifier or evidence `(kind, reference)` changes, the digest changes
    - Assert that when entries are not sorted by feature ID, the test suite fails (sort enforcement)
    - Test framework: deterministic unit tests
    - Test location: `crates/tokeira-compatibility/src/digest.rs` `#[cfg(test)]` module

  - [x] 2.7 Declare `SDK_MATRIX`
    - In `src/sdk.rs`, define `SdkVerificationState`, `SdkCompatEntry`, `IncompatibleVersion` structs per design
    - Declare `pub const SDK_MATRIX: &[SdkCompatEntry]` with five initial languages (Go, TypeScript, Python, Java, .NET)
    - Conservative verification states per Requirement 19
    - _Requirements: 19.1–19.13, 20.1–20.5_

  - [x]* 2.8 Write property test: SDK matrix JSON round-trip (P4)
    - **Property P4: SDK Matrix JSON Round-Trip**
    - **Validates: Requirements 53**
    - Serialise `SDK_MATRIX` via `serde_json::to_string`
    - Deserialise into owned representation
    - Assert structural equality; assert digest unchanged post-round-trip
    - Test framework: unit test (deterministic, no generation needed)
    - Test location: `crates/tokeira-compatibility/src/sdk.rs` `#[cfg(test)]` module

  - [x]* 2.9 Write property test: SDK version ordering (P5)
    - **Property P5: SDK Matrix Version Ordering**
    - **Validates: Requirements 21**
    - For every entry, parse `min_version` and known `max_tested_version` values as semver-like triples; assert `min <= max`
    - Assert every known-incompatible version includes a reason
    - Assert `max_tested_version` is not listed as known incompatible
    - Test framework: unit test (deterministic)
    - Test location: `crates/tokeira-compatibility/src/sdk.rs` `#[cfg(test)]` module

  - [x] 2.10 Implement `dispatch_rpc` helper
    - In `src/dispatch.rs`, define `DynamicConfigReader` trait, `DispatchMetrics` trait
    - Define `DispatchOutcome` and `DisabledReason` enums per design
    - Implement `pub fn dispatch_rpc<F: Feature>(dynamic_config, namespace, metrics) -> DispatchOutcome`
    - Each state maps to the correct outcome; `Experimental` checks dynamic config
    - Metrics incremented on every call
    - _Requirements: 18.1–18.7_

  - [x]* 2.11 Write unit tests for `dispatch_rpc` state handling
    - Four tests, one per `FeatureState` variant, using test features declared via `declare_feature!`
    - Mock `DynamicConfigReader` and `DispatchMetrics`
    - For `Experimental`: test both config-enabled and config-disabled paths
    - Assert correct `DispatchOutcome` variant and metric increment
    - Test location: `crates/tokeira-compatibility/src/dispatch.rs` `#[cfg(test)]` module
    - _Requirements: 18.1–18.7_

  - [ ]* 2.12 Write compile-fail tests for `cfg_feature!`
    - Use `trybuild` to create compile-fail fixtures: one gating a `Stubbed` feature, one gating an `Unsupported` feature
    - Add a compile-pass fixture gating an `Implemented` feature
    - Test location: `crates/tokeira-compatibility/tests/cfg_feature_compile.rs` + `tests/compile/`
    - _Requirements: 15.1 (compile-time gate enforcement)_

  - [x] 2.13 Checkpoint — compatibility crate compiles and tests pass
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-compatibility`
    - Verified with focused command: `cargo test -p tokeira-build-info -p tokeira-compatibility`

- [ ] 3. Matrix completeness and capability consistency properties
  - [ ] 3.1 Generate RPC name lists from vendored proto set
    - Add a build step or static declaration that enumerates every RPC in vendored `WorkflowService` and `OperatorService`
    - Emit `ALL_WORKFLOW_SERVICE_RPCS` and `ALL_OPERATOR_SERVICE_RPCS` as `&'static [&'static str]`
    - Also enumerate every field in the upstream `Capabilities` message
    - _Requirements: 17.1–17.6, 27.1–27.5_

  - [x]* 3.2 Write property test: matrix completeness (P1)
    - **Property P1: Matrix Completeness**
    - **Validates: Requirements 17**
    - For each RPC in `ALL_WORKFLOW_SERVICE_RPCS`, assert exactly one `FeatureEntry` owns it
    - For each RPC in `ALL_OPERATOR_SERVICE_RPCS`, assert exactly one `FeatureEntry` owns it
    - For each RPC referenced by any `FeatureEntry`, assert it exists in one of the two RPC lists
    - Test framework: unit test (deterministic enumeration)
    - Test location: `crates/tokeira-compatibility/src/matrix.rs` `#[cfg(test)]` module

  - [x]* 3.3 Write property test: capability consistency (P2)
    - **Property P2: Capability Consistency**
    - **Validates: Requirements 27**
    - For each field in upstream `GetSystemInfoResponse.Capabilities`, assert exactly one `FeatureEntry` maps to it via `capability_fields()`, or it is explicitly documented as intentionally unmapped
    - For each mapped field, assert it exists in the current `Capabilities` message or is explicitly listed as a future capability field pending proto sync
    - Test framework: unit test (deterministic enumeration)
    - Test location: `crates/tokeira-compatibility/src/matrix.rs` `#[cfg(test)]` module

  - [ ]* 3.4 Write property test: baseline flag agreement (P3)
    - **Property P3: Baseline Flag Agreement**
    - **Validates: Requirements 25**
    - With a dynamic-config reader that always returns `false`: every `capabilities.*` flag is `true` iff the matching feature is `Implemented`
    - Test framework: unit test (deterministic with mock config)
    - Test location: `crates/tokeira-compatibility/tests/baseline_flags.rs`

- [ ] 4. Kernel adoption — `cfg_feature!` gates
  - [ ] 4.1 Wrap existing feature-gated kernel modules
    - For each kernel module implementing an `Implemented` or `Experimental` feature, wrap the module declaration in `tokeira_compatibility::cfg_feature!("feature-id" => pub mod name { ... });`
    - Start with unambiguously implemented features; leave `Experimental` gates for a subsequent pass
    - No behaviour change — just compile-time assertions that the matrix agrees with the code
    - _Requirements: 15.1 (kernel compile-time gates)_

  - [ ]* 4.2 Write compile-fail test: flipping a feature to Stubbed breaks the kernel build
    - Use `trybuild` to verify that if a cfg-gated feature is set to `Stubbed`, the kernel fails to compile
    - Guards against accidental matrix flips without removing kernel code
    - Test location: `crates/tokeira-kernel/tests/feature_gate_regression.rs`

- [ ] 5. Edge adoption — `dispatch_rpc` for all handlers
  - [ ] 5.1 Declare features at handler module boundaries
    - For every workflow-service and operator-service handler, add `declare_feature!(FeatureStruct, "feature-id")` at the top
    - Feature ID must match the `FEATURE_MATRIX` entry owning the handler's RPC
    - _Requirements: 18.1_

  - [ ] 5.2 Route handlers through `dispatch_rpc`
    - Each handler's first statement: `let outcome = dispatch_rpc::<MyFeature>(dynamic_config, namespace, metrics);`
    - `Proceed` → fall through to existing handler logic
    - `Disabled { reason: Stubbed }` → `tonic::Status::unimplemented`
    - `Disabled { reason: Unsupported }` → `tonic::Status::unimplemented`
    - `Disabled { reason: ExperimentalDisabled }` → `tonic::Status::failed_precondition`
    - Emit metric tagged with feature ID and state on every dispatch
    - _Requirements: 18.1–18.7_

  - [ ]* 5.3 Write integration test for dispatch behaviour
    - Start an in-process instance with dynamic-config returning `false` for all keys
    - Call a `Stubbed` feature handler → assert `Unimplemented` status
    - Call an `Experimental` feature handler with config `false` → assert `FailedPrecondition`
    - Call an `Implemented` feature handler → assert it proceeds
    - Test location: `crates/tokeira-edge/tests/dispatch_integration.rs`
    - _Requirements: 18.2–18.6_

- [x] 6. Standard `GetSystemInfo` handler (upstream-only)
  - [x] 6.1 Implement the handler
    - In `crates/tokeira-edge/src/workflow_service.rs`, preserve the existing hardcoded `SystemCapabilities` baseline returned by `get_system_info`
    - Walk `FEATURE_MATRIX`; for each entry capability field, apply the compatibility overlay without changing currently true baseline flags
    - `Partial`, `Implemented`, and `Experimental` entries do not alter the baseline; `Stubbed`/`Unsupported` entries only preserve already-false flags
    - Add `TODO(temporal-compatibility)` documenting that the hardcoded baseline can be removed once the matrix has full conformance evidence for every capability
    - Set `server_version = TEMPORAL_SERVER_COMPAT`
    - Return ONLY upstream `GetSystemInfoResponse` fields — NO Tokeira-specific fields
    - _Requirements: 23.1–23.8, 24.1–24.4, 25.1–25.9_

  - [x] 6.2 Implement `set_capability_field` helper
    - A `match` over known capability field names; the current overlay preserves the hardcoded baseline while accepting matrix-owned field names
    - _Requirements: 25.1–25.5_

  - [x]* 6.3 Write unit test for handler output
    - Construct baseline capabilities matching the current `GetSystemInfo` response
    - Assert the matrix overlay preserves existing true baseline flags
    - Assert experimental and unmapped matrix entries do not alter existing false baseline flags
    - Assert a mapped `Stubbed` capability preserves an already-true baseline flag per the audit-informed MVP contract
    - Test location: `crates/tokeira-edge/src/workflow_service.rs` `#[cfg(test)]` module
    - _Requirements: 23.7, 24.1, 25.1–25.9_

  - [x]* 6.4 Write property test: standard handshake wire-shape (P7)
    - **Property P7: Standard Handshake Wire-Shape**
    - **Validates: Requirements 51**
    - Verify vendored `GetSystemInfoRequest`, `GetSystemInfoResponse`, and `Capabilities` descriptors match upstream for the pinned proto version
    - Any Tokeira-specific field in an upstream message fails the test
    - Test framework: unit test (deterministic descriptor comparison)
    - Test location: `crates/tokeira-edge/src/grpc/translate.rs` `#[cfg(test)]` module

- [ ] 7. Tokeira Compatibility Service (Buffa + connect-rust)
  - [x] 7.1 Create `proto/tokeira/compatibility/v1/compatibility.proto`
    - Define `package tokeira.compatibility.v1`
    - Define `CompatibilityService` with `GetCompatibility` RPC (required for MVP)
    - Define `GetCompatibilityRequest`, `GetCompatibilityResponse`, `BuildInfo`, `FeatureStateEntry`, `FeatureState` enum, `SdkCompatibilityEntry`, `KnownDivergence`, `CompatibilitySurface` messages per design
    - Proto file lives OUTSIDE the vendored upstream Temporal proto tree
    - _Requirements: 8.1–8.8, 28.1–28.9, 29.1–29.10_

  - [x] 7.2 Scaffold `crates/tokeira-compatibility-proto/`
    - Create crate with Buffa code generation for messages and connect-rust code generation for service traits/clients
    - Generated code checked in; freshness validated in CI
    - NOT tonic-generated, NOT prost-generated
    - _Requirements: 9.1–9.8, 10.1–10.8, 11.1–11.7_

  - [ ] 7.3 Generate Buffa messages and connect-rust service code
    - Run Buffa codegen for `tokeira.compatibility.v1` messages
    - Run connect-rust codegen for `CompatibilityService` trait and client
    - Check in generated code
    - Pin Buffa and connect-rust codegen tool versions in checked-in configuration
    - _Requirements: 8.3–8.6, 12.1–12.7_

  - [x] 7.4 Scaffold `crates/tokeira-compatibility-service/`
    - Create crate depending on `tokeira-compatibility`, `tokeira-build-info`, `tokeira-compatibility-proto`
    - Implement `GetCompatibility` handler: map `FEATURE_MATRIX` and `SDK_MATRIX` into Buffa DTOs
    - Populate `BuildInfo`, `process_kind`, `process_identity`, feature states, SDK entries, known divergences
    - Handle namespace parameter (global/default when absent)
    - _Requirements: 28.1–28.9, 29.1–29.10, 30.1–30.7_

  - [ ]* 7.5 Write property test: Buffa/connect-rust stack enforcement (P8)
    - **Property P8: Buffa/connect-rust Stack Enforcement**
    - **Validates: Requirements 52**
    - Assert Tokeira compatibility message types import from Buffa-generated modules
    - Assert Tokeira compatibility service code imports from connect-rust-generated modules
    - Assert generated code is fresh (regenerate and diff)
    - Test framework: unit test + CI freshness check
    - Test location: `crates/tokeira-compatibility-service/tests/stack_enforcement.rs`

  - [x]* 7.6 Write unit tests for `GetCompatibility` handler
    - Call handler with no namespace; assert response contains all `BuildInfo` fields, feature states, SDK entries
    - Call handler with a namespace; assert namespace-specific state if applicable
    - Current implementation has no namespace-specific matrix state, so tests validate global/default behavior and filtered feature/SDK lookup
    - Assert `process_kind` is populated
    - Test location: `crates/tokeira-compatibility-service/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 29.1–29.10_

  - [ ] 7.7 Wire compatibility service into all deployed processes
    - `tokeirad`, `tokeira-controller`, and `tokeira-autoscaler` each expose the compatibility service via connect-rust
    - Edge and projection metadata is exposed through `tokeirad`, because `tokeira-edge` and `tokeira-projection` are embedded crates rather than standalone deployed processes
    - Each process sets its own `process_kind` and `process_identity`
    - _Requirements: 30.1–30.7_

- [ ] 8. CLI adoption — `tkr compat show` and `tkr compat diff`
  - [x] 8.1 Add `CompatCommand` enum to `apps/tkr/src/cli.rs`
    - Add `Compat(CompatArgs)` variant to the top-level `Command` enum
    - Define `CompatCommand { Show { remote, json, verbose }, Diff { a, b, fail_on_incompatible } }`
    - _Requirements: 37.1, 38.1_

  - [ ] 8.2 Implement `tkr compat show` handler
    - Create `apps/tkr/src/commands/compat.rs`
    - Without `--remote`: print local build metadata from compile-time constants; display feature states and SDK entries
    - With `--remote`: call standard `GetSystemInfo` AND the Tokeira Compatibility Service (connect-rust client)
    - Graceful degradation: if Tokeira service unavailable, show standard `server_version` and explain
    - Support human-readable and JSON output
    - _Requirements: 37.1–37.19, 31.1–31.5_

  - [ ] 8.3 Implement `tkr compat diff` handler
    - Compare two local JSON documents, or local vs remote, or two remote deployments
    - Highlight changed versions, proto versions, server compat claims, feature states, SDK entries, source-tree hashes
    - Exit non-zero when `--fail-on-incompatible` is supplied and an incompatible difference is detected
    - _Requirements: 38.1–38.12_

  - [x] 8.4 Wire `compat` command into `apps/tkr/src/main.rs`
    - Dispatch to `commands::compat::run(args.command, format)`
    - _Requirements: 37.1_

  - [x]* 8.5 Write CLI parse tests
    - Parse `tkr compat show`, `tkr compat show --remote grpc://example:7233 --json`
    - Parse `tkr compat diff --a grpc://a:7233 --b grpc://b:7233`
    - Parse `tkr compat diff --fail-on-incompatible`
    - Test location: `apps/tkr/src/commands/compat.rs` `#[cfg(test)]` module

  - [ ]* 8.6 Write integration test: local vs remote consistency
    - Start in-process instance; call `tkr compat show` (local) and `tkr compat show --remote` against it
    - Parse both JSON outputs; assert static fields are equal
    - Test location: `apps/tkr/tests/compat_local_vs_remote.rs`
    - _Requirements: 37.2–37.5_

- [ ] 9. CLI adoption — `tkr ci check` and `tkr ci build`
  - [x] 9.1 Scaffold `apps/tkr/src/commands/ci/`
    - Create `mod.rs` with `CiCommand { Check { json, update_lock }, Build { versioned, json }, LockUpdate { json } }`
    - Add `Ci(CiArgs)` variant to `apps/tkr/src/cli.rs::Command`
    - Wire dispatcher in `apps/tkr/src/main.rs`
    - _Requirements: 44.1–44.8, 46.1, 47.1_
    - **DONE (2026-08-27, PR #133):** the scaffold now delegates every verb to the real
      `tokeira-build` CI pipeline rather than the former honest stub.

  - [x] 9.2 Implement `tkr ci check`
    - Invoke the Dagger compatibility `check` function
    - Use frozen lock mode by default
    - Open an isolated in-process Dagger session against the pinned runner
    - When Dagger unavailable: fail with clear setup message
    - When checks fail: return non-zero exit code
    - When checks pass: print concise success summary
    - When the user supplies `--update-lock`, the command MAY delegate to `tkr ci lock-update`
    - Support JSON output
    - _Requirements: 46.1–46.9_
    - **DONE (2026-08-27, PR #133):** check runs the reusable serde-report pipeline in-process,
      supports exact JSON and selected checks, delegates `--update-lock`, and exits non-zero on a
      failed result without implicitly provisioning an engine.

  - [x] 9.3 Implement `tkr ci build`
    - Without flags: invoke Dagger `dev` build function
    - With `--versioned`: invoke Dagger versioned build function (requires clean git, generates manifest, validates embedded BuildInfo)
    - When Dagger unavailable: fail with clear setup message
    - Do NOT use ambient environment variables as build metadata inputs
    - Support JSON output
    - _Requirements: 47.1–47.9_
    - **DONE (2026-08-27, PR #133):** dev and versioned builds run inside Dagger; the versioned
      path rejects dirt, derives its manifest twice inside the container, and validates every
      embedded BuildInfo field before export.

  - [x]* 9.4 Write CLI parse tests for `tkr ci`
    - Parse `tkr ci check`, `tkr ci check --json`
    - Parse `tkr ci build`, `tkr ci build --versioned`
    - Parse `tkr ci lock-update`, `tkr ci lock-update --json`
    - Test location: `apps/tkr/src/commands/ci/mod.rs` `#[cfg(test)]` module
    - **DONE (2026-08-27, PR #133):** parse coverage now includes all three verbs, JSON,
      `--versioned`, and repeatable `--check` selection.

  - [ ]* 9.5 Write integration test for `tkr ci check`
    - Invoke `tkr ci check` against a clean working tree; assert exit 0
    - Test location: `apps/tkr/tests/ci_check.rs` (gated behind `integration-test` feature)
    - _Requirements: 46.6–46.7_

  - [x] 9.6 Implement `tkr ci lock-update`
    - Invoke Dagger's explicit lock update mechanism or equivalent live-resolution mode
    - Run compatibility checks after lockfile changes
    - Print changed container image references, Git references, and HTTP fetch references
    - Support JSON output
    - Ensure normal `tkr ci check` and versioned build paths do not refresh `dagger.lock`
    - _Requirements: 44.1–44.8, 46.4_
    - **DONE (2026-08-27, PR #133):** live resolution writes the reviewed root `dagger.lock`,
      classifies changed container/Git/HTTP inputs, runs the checks, and rejects every other host
      working-tree mutation; normal check/build sessions remain frozen.

- [ ] 10. Dagger CI pipeline — compatibility module
  - [ ] 10.1 Scaffold Dagger compatibility module
    - Create the Dagger module exposing a `check` function
    - The `check` function runs all compatibility checks in a deterministic container
    - _Requirements: 40.10, 41.1–41.13_

  - [ ] 10.2 Implement no-wallclock check
    - Grep for `SystemTime::now|Utc::now|Local::now|OffsetDateTime::now_utc` in `crates/tokeira-build-info/`
    - Hits present = check FAILED; no hits = check PASSED
    - _Requirements: 48.5 (wall-clock detection)_

  - [ ] 10.3 Implement proto monotonicity check
    - Compare `TEMPORAL_PROTO_VERSION` and `TEMPORAL_SERVER_COMPAT` against base branch
    - Fail if tip version is lower than base (silent downgrade)
    - Allow explicit downgrade override via commit trailer
    - _Requirements: 35.1–35.5_

  - [ ] 10.4 Implement generated-code freshness checks
    - Regenerate Buffa code and connect-rust code; diff against checked-in versions
    - Regenerate upstream Temporal proto code; diff against checked-in versions
    - Any diff = check FAILED
    - This subtask depends on task 7.3, because the Tokeira compatibility Buffa/connect-rust outputs must exist before freshness can be checked
    - _Requirements: 8.5, 12.5–12.6, 41.8–41.10_

  - [ ] 10.5 Implement feature matrix and SDK matrix checks
    - Run matrix sort enforcement
    - Run digest stability check
    - Run SDK version ordering check
    - _Requirements: 41.5–41.6, 49.5_

  - [ ] 10.6 Implement source-tree hash check
    - Compute source-tree hash; verify determinism (compute twice, assert equal)
    - Verify excluded files don't affect hash; included files do
    - _Requirements: 5.1–5.12, 48.1–48.4_

  - [ ]* 10.7 Write property test: Dagger frozen-lock (P10)
    - **Property P10: Dagger Frozen-Lock**
    - **Validates: Requirements 54**
    - Assert hardened CI uses frozen lock mode
    - Assert missing lockfile entries fail
    - Assert modified lockfile during normal check fails
    - Test framework: unit test (deterministic)
    - Test location: `crates/tokeira-build/tests/frozen_lock.rs`

- [ ] 11. Dagger versioned build and lockfile policy
  - [ ] 11.1 Implement Dagger versioned build function
    - Derive all metadata from repository state and checked-in configuration
    - Generate the build metadata manifest
    - Invoke Cargo with `TOKEIRA_BUILD_MANIFEST_PATH` pointing to the manifest
    - Verify embedded `BuildInfo` after build
    - Reject dirty repository state
    - Reject non-deterministic source-tree hash
    - Do NOT use ambient environment variables as metadata inputs
    - _Requirements: 40.1–40.15, 42.1–42.11_

  - [ ] 11.2 Implement Dagger dev build function
    - Invoke Cargo without a manifest path
    - `build.rs` falls back to workspace-derived metadata
    - Allow dirty state and missing git provenance
    - _Requirements: 40.2–40.4_

  - [ ] 11.3 Implement lockfile policy
    - Commit `.dagger/lock` and Dagger module configuration
    - Hardened CI uses frozen lock mode
    - Normal checks fail if `.dagger/lock` is modified
    - Explicit lock update workflow for dependency refresh
    - Versioned build path uses frozen lock mode
    - _Requirements: 43.1–43.9, 44.1–44.8, 45.1–45.8_

  - [ ] 11.4 Implement source-tree hash computation
    - SHA-256 digest with deterministic file ordering
    - Include relative file paths and file contents
    - Exclude build artefacts, editor metadata, OS junk, local env files, Dagger runtime caches
    - Exclusion list declared in one checked-in location
    - Same exclusion list used by Dagger pipeline and local validation
    - _Requirements: 5.1–5.12_

- [x] 12. Startup provenance log and `--version` output
  - [x] 12.1 Extend `tokeirad` startup to emit build-info log entry
    - At earliest point after `tracing_subscriber::init()`, emit structured log with all `BuildInfo` fields
    - Include `feature_matrix_digest` and `sdk_matrix_digest`
    - Do NOT truncate hashes or version strings
    - Do NOT include wall-clock build timestamps
    - _Requirements: 7.1–7.6_

  - [x] 12.2 Extend all other processes with startup log
    - `tokeira-controller` and `tokeira-autoscaler` each emit one structured startup log event containing `BuildInfo`
    - Edge and projection metadata is covered by the `tokeirad` startup log in task 12.1 because those components are embedded in `tokeirad`
    - _Requirements: 7.2–7.3_

  - [x] 12.3 Implement `tokeirad --version`, `--version --verbose`, `--version --json`
    - Short form: `TOKEIRA_VERSION`, git SHA, build mode
    - Verbose form: all `BuildInfo` fields
    - JSON form: stable JSON representation with stable field names
    - JSON rendering implemented outside `tokeira-build-info`
    - Do NOT include wall-clock build timestamps
    - _Requirements: 6.1–6.8_

  - [x]* 12.4 Write integration test for `tokeirad --version` determinism
    - Invoke `tokeirad --version` twice; assert byte-equal stdout
    - Same for `--version --verbose` and `--version --json`
    - Test location: `apps/tokeirad/tests/version_cli.rs`
    - _Requirements: 6.5–6.6_

- [x] 13. Documentation
  - [x] 13.1 Update `README.md` with compatibility section
    - Document `TEMPORAL_SERVER_COMPAT`, `TEMPORAL_PROTO_VERSION`
    - Explain that proto compatibility and server behavioural compatibility are separate
    - Explain feature states and SDK verification states
    - Explain that Tokeira metadata is exposed through Buffa/connect-rust services, not patched Temporal protos
    - Explain that build metadata is derived through Dagger, not environment variables
    - Include examples of `tkr compat show` and `tkr compat diff`
    - _Requirements: 55.1–55.11_

  - [x] 13.2 Document Buffa/connect-rust guidance
    - Document that Tokeira-owned build/capability RPCs use Buffa and connect-rust
    - Document that upstream Temporal protos remain separate
    - Document how to regenerate code and how freshness is checked
    - Document how codegen tool versions are pinned
    - _Requirements: 56.1–56.7_

  - [x] 13.3 Document Dagger build and CI guidance
    - Document `tkr ci check` and `tkr ci build`
    - Document the versioned build path and metadata derivation
    - Document the generated build metadata manifest format
    - Document the Dagger lockfile policy and how to refresh `.dagger/lock`
    - Document why frozen lock mode is used
    - Document limitations of Dagger lockfiles
    - _Requirements: 57.1–57.12_

  - [x] 13.4 Create compatibility bump checklists
    - `TEMPORAL_PROTO_VERSION` bump checklist
    - `TEMPORAL_SERVER_COMPAT` bump checklist
    - Include upstream version, generated-code status, surface classification, conformance evidence, SDK matrix impact, known divergences
    - _Requirements: 58.1–58.10_

  - [x] 13.5 Update `AGENTS.md`
    - Add proto bump workflow to "Working Agreements"
    - Add server-compat independence rule
    - Add `tkr ci check` to Enforced Commands list
    - Add pointer from "adding a new feature" checklist to the matrix declaration
    - _Requirements: 55.1, 56.1, 57.1_

  - [x] 13.6 Add `README.md` to `crates/tokeira-build-info/`
    - Document the build metadata manifest format
    - Document dev mode fallback behaviour
    - Document the `pinned.rs` bump workflow
    - _Requirements: 3.11_

- [ ] 14. Final checkpoint — full workspace verification
  - [ ] 14.1 Run full verification suite
    - `cargo +nightly fmt --all --check`
    - `cargo lint`
    - `cargo test-lint`
    - `cargo check --workspace`
    - `cargo test --workspace`
    - `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"`
    - `tkr ci check` against a clean working tree (expected: all checks PASSED, exit 0)
    - All commands must pass with zero warnings

## Task Dependency Graph

```json
{
  "waves": [
    {
      "name": "Wave 1 — Foundation crates",
      "tasks": ["1"],
      "description": "Scaffold tokeira-build-info with manifest-based metadata"
    },
    {
      "name": "Wave 2 — Compatibility model",
      "tasks": ["2"],
      "description": "Scaffold tokeira-compatibility with feature matrix, SDK matrix, dispatch, macros, digests",
      "dependsOn": ["1"]
    },
    {
      "name": "Wave 3 — Properties and adoption (parallel tracks)",
      "tasks": ["3", "4", "5", "6", "7", "10.1-10.3", "10.5-10.7", "12"],
      "description": "Matrix completeness properties (3), kernel cfg_feature! (4), edge dispatch_rpc (5), GetSystemInfo handler (6), Tokeira Compatibility Service (7), Dagger CI pipeline excluding 10.4 (10.1-10.3, 10.5-10.7), startup log (12) — all depend on Wave 2 or Wave 1 and can proceed in parallel",
      "dependsOn": ["2"]
    },
    {
      "name": "Wave 4 — CLI and versioned build",
      "tasks": ["8", "10.4", "11"],
      "description": "tkr compat show/diff (8) depends on 6+7; generated-code freshness (10.4) depends on 7.3; Dagger versioned build (11) depends on the Dagger CI foundation",
      "dependsOn": ["3", "6", "7", "10.1-10.3", "10.5-10.7"]
    },
    {
      "name": "Wave 5 — CI CLI",
      "tasks": ["9"],
      "description": "tkr ci check/build/lock-update (9) depends on the Dagger check foundation (10) and Dagger build functions (11)",
      "dependsOn": ["10.1-10.3", "10.4", "10.5-10.7", "11"]
    },
    {
      "name": "Wave 6 — Documentation and verification",
      "tasks": ["13", "14"],
      "description": "Documentation (13) and final checkpoint (14) depend on all prior waves including generated-code freshness (10.4)",
      "dependsOn": ["8", "9", "10.4", "11", "12"]
    }
  ]
}
```

## Notes

- Property-based tests are marked with `*` in the task list and use `proptest` as the testing framework
- All property tests map to correctness properties P1–P10 from the design document
- `tkr compat bump` is explicitly deferred — this plan covers only inspection and CI guardrails
- The Tokeira Compatibility Service uses Buffa + connect-rust (NOT tonic/prost) per the design principle of consistent Tokeira-owned RPC stack
- Standard `GetSystemInfo` remains upstream-only with NO Tokeira-specific extension fields
- Build metadata is derived from a Dagger-generated manifest (versioned mode) or workspace fallback (dev mode) — never from ambient environment variables
