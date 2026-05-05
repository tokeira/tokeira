# Implementation Plan: Temporal Compatibility

## Overview

Embed compatibility metadata in the `tokeirad` binary, declare the feature and SDK matrices as single sources of truth, expose them via `GetSystemInfo` and `tkr compat show|diff`, and add CI checks that prevent silent drift.

Target crates:

- `crates/tokeira-build-info/` — NEW library crate with compile-time metadata constants
- `crates/tokeira-compatibility/` — NEW library crate with `FEATURE_MATRIX`, `SDK_MATRIX`, `dispatch_rpc`, `cfg_feature!`
- `crates/tokeira-edge/` — extend with `GetSystemInfo` handler that walks the matrix + `dispatch_rpc` adoption across workflow-service and operator-service handlers
- `crates/tokeira-kernel/` — adopt `cfg_feature!` at existing feature-gated module boundaries
- `apps/tkr/` — add `compat` command group (`show`, `diff`)
- `proto/tokeira/internal/v1/` — define `system_info_ext.proto` extension carrying tokeira build info
- `dev/ci/` — shell scripts for wall-clock check and proto monotonicity check

Crucially, this plan does **not** introduce dynamic config itself (the dynamic-config reader trait is injected), does not invent a new gRPC, and does not change any existing workflow semantics.

## Tasks

- [ ] 1. Scaffold `crates/tokeira-build-info/`
  - [ ] 1.1 Create the crate with zero runtime dependencies
    - Create `crates/tokeira-build-info/Cargo.toml` with `[build-dependencies]` only (`toml` for parsing `rust-toolchain.toml`). The `[dependencies]` section is empty
    - Add `"crates/tokeira-build-info"` to `[workspace.members]` in the root `Cargo.toml`
    - In `crates/tokeira-build-info/src/lib.rs`, define the `BuildInfo` struct and the six public constants (`TOKEIRA_VERSION`, `TOKEIRA_GIT_SHA`, `TEMPORAL_PROTO_VERSION`, `TEMPORAL_SERVER_COMPAT`, `RUST_TOOLCHAIN`, `SOURCE_TREE_HASH`) each bound via `env!("TOKEIRA_BUILD_INFO_…")`
    - Add a `pub const fn summary() -> BuildInfo` returning the struct populated from the constants
    - _Requirements: 1.1, 9.2_

  - [ ] 1.2 Create `src/pinned.rs`
    - Declare `pub const TEMPORAL_PROTO_VERSION: &str = "v1.47.0";` and `pub const TEMPORAL_SERVER_COMPAT: &str = "1.27.0";` with a doc comment pointing at the spec
    - _Requirements: 5.1, 5.3_

  - [ ] 1.3 Implement `build.rs`
    - Create `crates/tokeira-build-info/build.rs`
    - Emit `cargo:rerun-if-env-changed` for `TOKEIRA_GIT_SHA`, `TOKEIRA_SOURCE_TREE_HASH`, `CI`, `CARGO_PROFILE`
    - Emit `cargo:rerun-if-changed` for `../../rust-toolchain.toml`, `../../Cargo.toml`, `src/pinned.rs`
    - Resolve `TOKEIRA_VERSION` from `CARGO_PKG_VERSION`
    - Resolve `TOKEIRA_GIT_SHA` per `resolve_git_sha` logic (env var → `git rev-parse --short=8` → fallback). Fail release-in-CI without env var; warn-but-succeed release-outside-CI without env var; substitute `dev` in debug
    - Parse `src/pinned.rs` as text via a small regex to extract the two version constants; fail fast if empty
    - Parse `../../rust-toolchain.toml` via the `toml` build-dep; extract `[toolchain] channel` or `[toolchain] version`; fail fast on missing file
    - Resolve `TOKEIRA_SOURCE_TREE_HASH` from env var; substitute 64-literal-zero string in debug; fail in release-CI without env var
    - Emit `cargo:rustc-env` for each of the six constants
    - **Do NOT** call `SystemTime::now`, `chrono::Utc::now`, `chrono::Local::now`, `OffsetDateTime::now_utc`, or any other wall-clock source (Req 1.6, 9.1)
    - _Requirements: 1.1.2, 1.2, 1.3, 1.6, 5.3, 6.1, 6.2, 9.1_

  - [ ]* 1.4 Write unit test for deterministic `--version` formatting (Property P-BI-1)
    - **Property P-BI-1: Version Output Determinism**
    - **Validates: Requirements 1.4, 8.5**
    - In `crates/tokeira-build-info/tests/version_format.rs`, define a canonical `BuildInfo` fixture
    - Call a pure formatter function `format_version_short(&BuildInfo) -> String` twice; assert byte-equal output
    - Call `format_version_verbose(&BuildInfo) -> String` twice; assert byte-equal output
    - Call `format_version_json(&BuildInfo) -> String` twice; assert byte-equal output
    - The formatter functions are defined in `src/format.rs`, pure, no I/O
    - _Requirements: 1.4, 8.5_

  - [ ]* 1.5 Write unit test for `build.rs` env-var resolution
    - **Property P-BI-2: Build-time Env Var Resolution**
    - **Validates: Requirements 1.2.1, 1.2.2, 1.2.3, 1.2.4, 6.2.1, 6.2.2**
    - Extract the decision logic from `build.rs` into a pure helper `fn decide_git_sha(env_value: Option<&str>, git_sha: Option<&str>, profile: &str, in_ci: bool) -> Result<String, BuildError>`
    - Test each combination: env present; env empty + git present; env empty + git absent + debug; env empty + git absent + release + CI (error); env empty + git absent + release + no CI (warn, returns `dev`)
    - Test location: `crates/tokeira-build-info/build.rs` `#[cfg(test)]` module or `crates/tokeira-build-info/src/build_logic.rs`
    - _Requirements: 1.2, 6.2_

  - [ ] 1.6 Checkpoint — workspace compiles with new crate
    - Run `cargo +nightly fmt`, `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-build-info`
    - _Requirements: 1.1_

- [ ] 2. Scaffold `crates/tokeira-compatibility/`
  - [ ] 2.1 Create the crate
    - Create `crates/tokeira-compatibility/Cargo.toml` with `thiserror`, `serde`, `tracing` as workspace-pinned deps, and `tokeira-build-info` as a path dep
    - Add `"crates/tokeira-compatibility"` to `[workspace.members]`
    - _Requirements: 2.2, 9.2_

  - [ ] 2.2 Define `FeatureState`, `FeatureEntry`, `Feature` trait
    - In `src/feature.rs`, define `FeatureState { Implemented, Experimental, Stubbed, Unsupported }` with `Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq`
    - Define `FeatureEntry` struct per the Design doc with `id`, `state`, `capability_field`, `dynamic_config_key`, `rpcs` (all `&'static` references — no heap)
    - Define `pub trait Feature { const ID: &'static str; const ENTRY: &'static FeatureEntry; }`
    - Define `pub const fn lookup_feature_const(id: &'static str) -> &'static FeatureEntry` with a compile-time linear scan over `FEATURE_MATRIX`; `panic!` on miss
    - Define a `pub const fn const_str_eq(a: &str, b: &str) -> bool` helper used by `lookup_feature_const`
    - _Requirements: 2.1, 2.2, 2.4_

  - [ ] 2.3 Declare `FEATURE_MATRIX`
    - In `src/matrix.rs`, declare `pub const FEATURE_MATRIX: &[FeatureEntry]` with the initial seed from Req 2.6, fully populated so every workflow-service and operator-service RPC maps to exactly one entry
    - Group entries by lifecycle (Implemented → Experimental → Stubbed → Unsupported) for readability; Order does not affect the digest (the digest sorts by id)
    - Every `Experimental` entry SHALL have a non-None `dynamic_config_key`; every other entry SHALL have `None`
    - Every entry's `rpcs` list SHALL be non-empty unless the feature is a cross-cutting capability (e.g., `workflow-namespaces` covers multiple RPCs)
    - _Requirements: 2.2, 2.3, 2.6_

  - [ ] 2.4 Implement matrix digest
    - In `src/digest.rs`, implement `pub const fn feature_matrix_digest() -> &'static str` using a `const fn` FNV-1a over `(id, state_label)` tuples, sorted by `id` at compile time
    - Also implement `pub const fn sdk_matrix_digest() -> &'static str` over `(language, min_version, max_tested_version)` tuples
    - Convert the digest bytes to a `&'static str` hex literal via a `const` array-of-bytes conversion helper
    - Expose `pub const FEATURE_MATRIX_DIGEST: &str` and `pub const SDK_MATRIX_DIGEST: &str`
    - _Requirements: 2.2.3, 3.2_

  - [ ]* 2.5 Write property test for matrix digest stability (Property P-COMPAT-1)
    - **Property P-COMPAT-1: Digest Stability**
    - **Validates: Requirements 2.2.3**
    - Compute `feature_matrix_digest()` twice in a test; assert byte-equal
    - Compute with a matrix whose entries are declared in a different order (via a test-only `FEATURE_MATRIX_SHUFFLED`); assert the digest equals the canonical `FEATURE_MATRIX_DIGEST`
    - Test location: `crates/tokeira-compatibility/src/digest.rs` `#[cfg(test)]` module

  - [ ] 2.6 Implement `declare_feature!` and `cfg_feature!` macros
    - In `src/macros.rs`, implement both `macro_rules!` macros per the Design doc
    - `declare_feature!($name:ident, $id:literal)` declares a zero-sized struct implementing `Feature` with `ID` and `ENTRY` const items
    - `cfg_feature!($feature_id:literal => $($tt:tt)*)` emits a `const _: () = { ... }` block that panics at compile time if the feature state is `Stubbed` or `Unsupported`, then emits `$($tt)*`
    - Export both macros with `#[macro_export]`
    - _Requirements: 2.4_

  - [ ]* 2.7 Write compile-fail tests for `cfg_feature!` (Property P-COMPAT-2)
    - **Property P-COMPAT-2: cfg_feature! Rejects Stubbed/Unsupported**
    - **Validates: Requirement 2.4.2**
    - Use `trybuild` (workspace-pinned dev-dep) to create two compile-fail fixtures: one gating a `Stubbed` feature, one gating an `Unsupported` feature
    - Assert each produces a compile error whose message contains the feature id
    - Add a compile-pass fixture gating an `Implemented` feature; assert it compiles clean
    - Test location: `crates/tokeira-compatibility/tests/cfg_feature_compile.rs` + `crates/tokeira-compatibility/tests/compile/`
    - _Requirements: 2.4.2_

  - [ ] 2.8 Declare `SDK_MATRIX`
    - In `src/sdk.rs`, define `SdkCompatEntry` and `IncompatibleVersion` structs per the Design doc
    - Declare `pub const SDK_MATRIX: &[SdkCompatEntry]` with the five initial languages (Go, TypeScript, Python, Java, .NET) and placeholder version ranges for the first release
    - _Requirements: 3.1_

  - [ ]* 2.9 Write property test for SDK matrix JSON round-trip (Property P-COMPAT-3)
    - **Property P-COMPAT-3: SDK Matrix Round-Trip**
    - **Validates: Requirements 3.3, 8.4**
    - Serialise `SDK_MATRIX` via `serde_json::to_string`
    - Deserialise into `Vec<SdkCompatEntry>` (owned copy of the `&'static` form)
    - Assert every field matches; assert recomputing the digest over the deserialised form produces the same `SDK_MATRIX_DIGEST`
    - Test location: `crates/tokeira-compatibility/src/sdk.rs` `#[cfg(test)]` module

  - [ ]* 2.10 Write property test for SDK version ordering (Property P-COMPAT-4)
    - **Property P-COMPAT-4: SDK Version Ordering**
    - **Validates: Requirement 3.3.1**
    - Iterate `SDK_MATRIX`; for each entry, parse `min_version` and `max_tested_version` via the `semver` crate (workspace-pinned dev-dep); assert `min_version <= max_tested_version`
    - Iterate `known_incompatible`; assert none of the incompatible versions equal `max_tested_version`
    - Test location: `crates/tokeira-compatibility/src/sdk.rs` `#[cfg(test)]` module
    - _Requirements: 3.3.2_

  - [ ] 2.11 Implement `dispatch_rpc` helper
    - In `src/dispatch.rs`, define `DynamicConfigReader` trait with `fn bool_for_namespace(&self, key: &str, namespace: Option<&str>) -> bool`
    - Define `DispatchMetrics` trait with `fn increment_dispatch(&self, feature_id: &str, state: FeatureState)`
    - Define `RpcDispatchContext<'a>` and `DispatchOutcome<T>` per the Design doc
    - Implement `pub fn dispatch_rpc<F: Feature>(ctx: &RpcDispatchContext<'_>) -> DispatchOutcome<()>` per the Design doc
    - _Requirements: 2.5_

  - [ ]* 2.12 Write unit tests for `dispatch_rpc` state handling
    - Four tests, one per `FeatureState` variant, using a test feature declared via `declare_feature!` for each state category
    - Use a mock `DynamicConfigReader` that returns canned values; mock `DispatchMetrics` records the call
    - For `Experimental`: test both config-enabled and config-disabled paths
    - Assert the correct `DispatchOutcome` variant and that the metric is incremented exactly once per call
    - Test location: `crates/tokeira-compatibility/src/dispatch.rs` `#[cfg(test)]` module
    - _Requirements: 2.5.1, 2.5.3_

  - [ ] 2.13 Checkpoint — compatibility crate compiles and tests pass
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-compatibility`

- [ ] 3. Matrix completeness property test
  - [ ] 3.1 Generate an RPC name list from the vendored proto set
    - Add a build step in `tokeira-compatibility/build.rs` that reads generated tonic stubs from `tokeira-proto` (or alternatively, statically list the RPCs by parsing the vendored `.proto` files via `prost-build`'s reflection)
    - Emit a `&'static [&'static str] ALL_WORKFLOW_SERVICE_RPCS` and `ALL_OPERATOR_SERVICE_RPCS` as generated code included via `include!`
    - _Requirements: 2.3, 8.1_

  - [ ]* 3.2 Implement completeness property test (Property P-COMPAT-5)
    - **Property P-COMPAT-5: Matrix Completeness**
    - **Validates: Requirements 2.3, 8.1**
    - Iterate `ALL_WORKFLOW_SERVICE_RPCS`; for each name, assert there exists exactly one `FeatureEntry` in `FEATURE_MATRIX` whose `rpcs` slice contains that name
    - Same for `ALL_OPERATOR_SERVICE_RPCS`
    - Collect every name in `FeatureEntry.rpcs` across all entries; assert each is present in one of the two RPC name lists (no orphan RPCs pointing at removed proto methods)
    - Test location: `crates/tokeira-compatibility/tests/matrix_completeness.rs`
    - _Requirements: 2.3, 8.1_

- [ ] 4. `GetSystemInfo` extension proto
  - [ ] 4.1 Define the extension message
    - In `proto/tokeira/internal/v1/system_info_ext.proto`, define `message TokeiraBuildInfoExt` with the six fields per the Design doc
    - Add the proto file to the internal proto compilation list owned by [`proto-upstream-sync`](../proto-upstream-sync/requirements.md)
    - _Requirements: 4.1.4, 4.1.5_

  - [ ] 4.2 Wire the extension fields into `GetSystemInfoResponse`
    - Owned by [`proto-upstream-sync`](../proto-upstream-sync/requirements.md) — the vendored proto definition carries `TokeiraBuildInfoExt tokeira_build_info = N;` and `map<string, string> tokeira_feature_states = N+1;` as optional unknown-field-tolerant extensions. This task captures the design constraint: the field numbers chosen SHALL be outside the 1-2**19 range reserved by the Temporal upstream schema to avoid future collisions
    - _Requirements: 4.1.4, 4.1.5_

  - [ ]* 4.3 Write property test for extension round-trip (Property P-COMPAT-6)
    - **Property P-COMPAT-6: Extension Field Round-Trip**
    - **Validates: Requirement 4.1.4**
    - Encode a populated `TokeiraBuildInfoExt` via prost, decode, assert every field matches
    - Test location: `crates/tokeira-compatibility/tests/extension_roundtrip.rs`

- [ ] 5. `GetSystemInfo` handler in `tokeira-edge`
  - [ ] 5.1 Implement the handler
    - In `crates/tokeira-edge/src/handlers/system_info.rs`, implement `async fn get_system_info(req, ctx) -> Result<GetSystemInfoResponse, tonic::Status>` per the Design doc
    - Walk `FEATURE_MATRIX`; for each entry with a `capability_field`, set the corresponding field on `Capabilities` (true for `Implemented`, dynamic-config-dependent for `Experimental`, false for `Stubbed`/`Unsupported`)
    - Populate `tokeira_feature_states` with every entry's (id, state label) pair
    - Populate `tokeira_build_info` with the six constants plus the two digests
    - Set `server_version = TEMPORAL_SERVER_COMPAT`
    - _Requirements: 4.1_

  - [ ] 5.2 Implement `set_capability_field`
    - A `match` over `Capabilities` field names; the proptest in task 5.4 enforces exhaustiveness
    - _Requirements: 4.1.3, 4.2_

  - [ ]* 5.3 Write unit test for handler output
    - Construct a handler context with a canned dynamic-config reader (all `false`)
    - Call `get_system_info`; assert every `Implemented` feature's `capabilities.*` flag is `true`; every `Experimental` feature's flag is `false`; every `Stubbed`/`Unsupported` feature's flag is `false`
    - Assert `server_version` equals `TEMPORAL_SERVER_COMPAT`
    - Assert `tokeira_build_info.feature_matrix_digest == FEATURE_MATRIX_DIGEST`
    - Test location: `crates/tokeira-edge/src/handlers/system_info.rs` `#[cfg(test)]` module
    - _Requirements: 4.1_

  - [ ]* 5.4 Write property test for capability consistency (Property P-COMPAT-7)
    - **Property P-COMPAT-7: Capability Consistency**
    - **Validates: Requirements 4.2, 8.2**
    - Enumerate every field name in `GetSystemInfoResponse.Capabilities` (via a code-generated name list built in task 3.1 or a separate proto-reflection pass)
    - For each name, assert there exists exactly one `FeatureEntry` with `capability_field == Some(name)`
    - With a dynamic-config reader that returns `false`, for every feature in `FEATURE_MATRIX`: if `state == Implemented` and `capability_field.is_some()`, the corresponding field on the handler response is `true`; otherwise `false`
    - Test location: `crates/tokeira-edge/tests/capability_consistency.rs`
    - _Requirements: 4.2, 8.2_

- [ ] 6. Adopt `dispatch_rpc` across edge handlers
  - [ ] 6.1 Declare features at handler module boundaries
    - For every workflow-service and operator-service handler module, add a `declare_feature!(FeatureStruct, "feature-id")` declaration at the top of the file. The feature id must match the `FEATURE_MATRIX` entry that owns the handler's RPC
    - _Requirements: 2.5.2_

  - [ ] 6.2 Route handlers through `dispatch_rpc`
    - Each handler's first statement SHALL be `let dispatch = dispatch_rpc::<MyFeature>(&dispatch_ctx);`
    - For `Proceed`, fall through to the existing handler logic
    - For `FailedPrecondition { message, details }`, return `Err(tonic::Status::failed_precondition(format!("{message}: {details}")))`
    - For `Unimplemented { message }`, return `Err(tonic::Status::unimplemented(message))`
    - _Requirements: 2.1, 2.5_

  - [ ]* 6.3 Write integration test for stubbed-feature dispatch
    - Start a tokeirad in-process instance with a dynamic-config reader that returns `false` for every key
    - Call a handler whose feature state is `Stubbed`; assert the response status is `Unimplemented` and the message contains the feature id
    - Call a handler whose feature state is `Experimental` with dynamic-config `false`; assert `FailedPrecondition` with the dynamic-config key in the message
    - Call a handler whose feature state is `Implemented`; assert it proceeds to the real logic (a minimal happy-path assertion)
    - Test location: `crates/tokeira-edge/tests/dispatch_integration.rs`
    - _Requirements: 2.1, 2.5_

- [ ] 7. Adopt `cfg_feature!` in the kernel
  - [ ] 7.1 Wrap existing feature-gated kernel modules
    - For each kernel module that implements an `Implemented` or `Experimental` feature, wrap the module declaration in `tokeira_compatibility::cfg_feature!("feature-id" => pub mod name { ... });`
    - Start with features that are unambiguously implemented (e.g., `workflow-queries`, `workflow-signals`); leave `Experimental` gates for a subsequent pass once the dynamic-config wiring is stable
    - _Requirements: 2.4.1, 2.4.2_

  - [ ]* 7.2 Write compile-fail test that flipping a feature to Stubbed breaks the kernel build
    - Add a `trybuild` fixture that artificially overrides `FEATURE_MATRIX` to set an `Implemented`-and-cfg-gated feature to `Stubbed`; assert the kernel fails to compile
    - This test is a guard against accidental matrix flips: if a maintainer tries to downgrade an implemented feature to stubbed without removing its kernel code, CI catches it
    - Test location: `crates/tokeira-kernel/tests/feature_gate_regression.rs` + `crates/tokeira-kernel/tests/compile/`
    - _Requirements: 2.4.2_

- [ ] 8. `tokeirad` startup log and `--version` output
  - [ ] 8.1 Extend `tokeirad` startup to emit the build-info log entry
    - At the earliest point after `tracing_subscriber::init()` in `apps/tokeirad/src/main.rs`, emit `tracing::info!(target: "tokeirad.startup", ... build_info fields ...)` per Req 1.5
    - Include `feature_matrix_digest = FEATURE_MATRIX_DIGEST` and `sdk_matrix_digest = SDK_MATRIX_DIGEST` as structured fields
    - _Requirements: 1.5_

  - [ ] 8.2 Implement `--version`, `--version --verbose`, `--version --json`
    - In `apps/tokeirad/src/cli.rs`, add a `Version { verbose: bool, json: bool }` arm to the top-level command enum
    - Short form: three lines per Req 1.4.1 using `format_version_short`
    - Verbose form: short form plus four additional lines per Req 1.4.2 using `format_version_verbose`
    - JSON form: single JSON object per Req 1.4.3 using `format_version_json`
    - All three formatters are pure `&BuildInfo -> String` helpers in `tokeira-build-info/src/format.rs`
    - _Requirements: 1.4_

  - [ ]* 8.3 Write integration test for `tokeirad --version` determinism
    - Invoke `tokeirad --version` via `std::process::Command` twice in sequence; assert the two stdouts are byte-equal
    - Do the same for `--version --verbose` and `--version --json`
    - Test location: `apps/tokeirad/tests/version_cli.rs`, gated behind `integration-test` feature per AGENTS.md testing guidance
    - _Requirements: 1.4, 8.5_

- [ ] 9. `tkr compat` command group
  - [ ] 9.1 Add `CompatCommand` enum to `apps/tkr/src/cli.rs`
    - Add `Compat(CompatArgs)` variant to the top-level `Command` enum
    - Define `CompatArgs` with a subcommand bound to `CompatCommand { Show { remote, json }, Diff { a, b, local } }`
    - _Requirements: 7.1_

  - [ ] 9.2 Implement the show handler
    - Create `apps/tkr/src/commands/compat.rs`. Implement `run(cmd, format)` per the Design doc
    - For `Show { remote: None, json }`: construct a `GetSystemInfoResponse`-shaped local view from compile-time constants via `build_local_response()`; render text or JSON
    - For `Show { remote: Some(endpoint), json }`: dial via the existing tkr gRPC client infrastructure, call `GetSystemInfo`, render
    - _Requirements: 7.1, 7.2_

  - [ ] 9.3 Implement the diff handler
    - For `Diff { a: Some(a), b: Some(b), local: None }`: `tokio::try_join!` two remote calls, produce a unified-diff-style output listing differing fields
    - For `Diff { a: None, b: None, local: Some(endpoint) }`: local view vs remote call
    - Exit status: 0 if identical on compared fields, 1 on any difference, 2 on usage error (enforced by clap)
    - Render text format; support `--json` via `format: OutputFormat` plumbed from tkr global flags
    - _Requirements: 7.3_

  - [ ] 9.4 Wire the `compat` command into `apps/tkr/src/main.rs`
    - Add `Command::Compat(args) => commands::compat::run(args.command, format).await?`
    - Position the command between existing `deployment` and other top-level groups per [`tkr-cli`](../tkr-cli/requirements.md) conventions
    - _Requirements: 7.1_

  - [ ]* 9.5 Write unit tests for CLI parse
    - Parse `tkr compat show`: assert `remote == None`, `json == false`
    - Parse `tkr compat show --remote grpc://example:7233 --json`: assert values match
    - Parse `tkr compat diff --a grpc://a:7233 --b grpc://b:7233`
    - Parse `tkr compat diff --local grpc://remote:7233`
    - Parse `tkr compat diff --a ... --local ...` (conflict): assert clap returns usage error
    - Test location: `apps/tkr/src/commands/compat.rs` `#[cfg(test)]` module

  - [ ]* 9.6 Write property test for local vs remote consistency (Property P-COMPAT-8)
    - **Property P-COMPAT-8: Local vs Remote Show Consistency**
    - **Validates: Requirement 7.2**
    - Start a tokeirad in-process instance; call `tkr compat show` (local) and `tkr compat show --remote <addr>` against it
    - Parse both JSON outputs; assert the static fields (build_info, feature_matrix_digest, sdk_matrix_digest, all features with state Implemented/Stubbed/Unsupported) are byte-equal
    - Note that `Experimental` state may differ if the running server has dynamic config enabled — assert the other fields are equal; test this case separately
    - Test location: `apps/tkr/tests/compat_local_vs_remote.rs`
    - _Requirements: 7.2_

- [ ] 10. CI checks
  - [ ] 10.1 Add `dev/ci/check-no-wallclock.sh`
    - Create the script per the Design doc
    - Make it executable; wire it into the CI workflow as a required check
    - _Requirements: 9.1_

  - [ ] 10.2 Add `dev/ci/check-proto-monotonicity.sh`
    - Create a bash script that: (a) reads the last git tag matching `v*`; (b) extracts `TEMPORAL_PROTO_VERSION` from `crates/tokeira-build-info/src/pinned.rs` at both the tip and the last tag; (c) compares via `sort -V -r | head -1` — if the tip version is less than the tagged version, fail unless the tip commit message contains `Proto-Downgrade:`
    - Do the same for `TEMPORAL_SERVER_COMPAT` with the override key `Server-Compat-Downgrade:`
    - Wire both into the CI workflow as required checks on PRs that modify `pinned.rs`
    - _Requirements: 5.4, 8.3_

  - [ ]* 10.3 Write a smoke test for the wall-clock check
    - Add a temporary wall-clock call to `tokeira-build-info/build.rs`; run `check-no-wallclock.sh`; assert it exits non-zero
    - Remove the temporary call; assert it exits zero
    - This can be a one-time manual verification rather than a CI test (document in the task checklist)
    - _Requirements: 9.1_

- [ ] 11. Source tree hash helper
  - [ ] 11.1 Implement `dev/ci/compute-source-tree-hash`
    - A small Rust binary (under `dev/ci/` or `tools/source-tree-hash/`) that walks the workspace, applies the exclusion list from Req 1.3.3, sorts paths, and prints a SHA-256 to stdout
    - Used by the Dagger pipeline (owned by [`image-lifecycle`](../image-lifecycle/requirements.md)) and by local `cargo build --release` workflows where the operator wants reproducible-build provenance
    - _Requirements: 1.3, 6.1_

  - [ ]* 11.2 Write property test for hash determinism
    - **Property P-CI-1: Source Tree Hash Determinism**
    - **Validates: Requirement 1.3.4**
    - Generate arbitrary file trees via `proptest` (bounded depth, bounded file sizes); hash twice; assert byte-equal
    - Shuffle the traversal order in a test-only alternate implementation; assert the sort produces the same hash
    - Test location: `tools/source-tree-hash/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 1.3.4_

- [ ] 12. Documentation and integration
  - [ ] 12.1 Update `README.md`
    - Add a "Temporal compatibility" section citing `TEMPORAL_SERVER_COMPAT`, `TEMPORAL_PROTO_VERSION`, summarising the feature matrix by state (e.g., "33 implemented, 5 experimental, 2 stubbed, 1 unsupported"), and pointing at `tkr compat show` for detail
    - _Requirements: 9.3.1_

  - [ ] 12.2 Update `AGENTS.md`
    - Add the proto bump workflow (Req 5.2) to "Working Agreements"
    - Add the server-compat independence rule (Req 5.3) to the same section
    - Add a pointer from any "adding a new feature" checklist to this spec's matrix declaration
    - _Requirements: 9.3.2_

  - [ ] 12.3 Add `README.md` to `crates/tokeira-build-info/`
    - Document the env vars a CI or hand-run release build must set (`TOKEIRA_GIT_SHA`, `TOKEIRA_SOURCE_TREE_HASH`, `CI`)
    - Document the debug build fallbacks
    - Document the `pinned.rs` bump workflow
    - _Requirements: 6.2.3_

  - [ ] 12.4 Final checkpoint — full workspace verification
    - Run `cargo +nightly fmt --all --check`
    - Run `cargo lint`
    - Run `cargo test-lint`
    - Run `cargo check --workspace`
    - Run `cargo test --workspace`
    - Run `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"`
    - Run `dev/ci/check-no-wallclock.sh`
    - Run `dev/ci/check-proto-monotonicity.sh` against a clean working tree (expected: no-op)
    - All commands must pass with zero warnings
