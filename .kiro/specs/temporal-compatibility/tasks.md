# Implementation Plan: Temporal Compatibility

## Overview

Embed compatibility metadata in the `tokeirad` binary, declare the feature and SDK matrices as single sources of truth, expose them via `GetSystemInfo` and `tkr compat show|diff`, and add CI checks that prevent silent drift.

Target crates:

- `crates/tokeira-build-info/` — NEW library crate with compile-time metadata constants
- `crates/tokeira-compatibility/` — NEW library crate with `FEATURE_MATRIX`, `SDK_MATRIX`, `dispatch_rpc`, `cfg_feature!`
- `crates/tokeira-edge/` — extend with `GetSystemInfo` handler that walks the matrix + `dispatch_rpc` adoption across workflow-service and operator-service handlers
- `crates/tokeira-kernel/` — adopt `cfg_feature!` at existing feature-gated module boundaries
- `apps/tkr/` — add `compat` and `ci` command groups; `compat` carries `show`, `diff`, and `bump`
- `proto/tokeira/internal/v1/` — define `system_info_ext.proto` extension carrying tokeira build info
- `crates/tokeira-build/src/pipelines/ci.rs` — NEW Dagger-backed local CI pipeline (no-wallclock, version-pin monotonicity, bump-trailer, source-tree hash)
- `crates/tokeira-build/src/compat_bump/` — NEW bump engine powering `tkr compat bump` (preflight / evidence / mutate / publish phases; octocrab-backed GitHub API integration)
- `.github/CODEOWNERS` — NEW file naming `pinned.rs` as compat-owner-gated
- `docs/compat-bumps/0-baseline.md` — NEW retroactive baseline establishing the starting point for bump PR diffs

Crucially, this plan does **not** introduce dynamic config itself (the dynamic-config reader trait is injected), does not invent a new gRPC, does not change any existing workflow semantics, and does not wire any remote CI triggers (GitHub Actions, nightly crons, release pipelines) — those are owned by the `pipeline-foundation` spec (backlog P16).

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

- [ ] 10. Local CI pipeline via Dagger
  - [ ] 10.1 Extract `dagger_reexec` helper into a shared module
    - Move the `should_reexec_under_dagger`, `reexec_under_dagger`, and `reexec_args` helpers out of `apps/tkr/src/commands/image/mod.rs` and into a new `apps/tkr/src/dagger_reexec.rs` module. Generalise `reexec_args` to take a `&[String]` of already-formatted argv tail rather than the `ImageCommand` enum, so both `image` and `ci` command groups can share it
    - Update `apps/tkr/src/commands/image/mod.rs` to import the shared helpers; add a small per-command shim that formats `ImageCommand` into `Vec<String>` before calling the shared `reexec_under_dagger`
    - _Requirements: 10.2.5, 10.4.3_

  - [ ] 10.2 Scaffold `crates/tokeira-build/src/pipelines/ci.rs`
    - Create the module alongside `pipelines/build.rs`, `pipelines/publish.rs`, `pipelines/mirror.rs`
    - Define the `CiCheck`, `CiCheckRequest`, `CiCheckReport`, `CiCheckResult` types per design.md §7, all deriving `Serialize + Deserialize`
    - Add `pub fn run_ci_checks(request: &CiCheckRequest, dagger: &dyn DaggerClient) -> Result<CiCheckReport, BuildError>` with the shared container-preamble (apt install `ripgrep` + `git`, workdir `/workspace`, `with_directory` using `TOKEIRAD_WORKSPACE_EXCLUDES`)
    - Re-export the public surface from `crates/tokeira-build/src/lib.rs`
    - _Requirements: 10.1, 10.4_

  - [ ] 10.3 Implement the no-wallclock check
    - Inside `ci.rs`, add `fn run_no_wallclock(base: &dyn ContainerRef<'_>) -> Result<CiCheckResult, BuildError>` that invokes `rg -n 'SystemTime::now|Utc::now|Local::now|OffsetDateTime::now_utc|chrono::Utc::now|chrono::Local::now' crates/tokeira-build-info/` inside the container and inspects the exit code
    - `rg` exit status 0 = hits present = check FAILED; exit status 1 = no hits = check PASSED; any other exit = surface as a `BuildError::Validation`
    - Capture the matching lines as `details` on the `CiCheckResult`
    - _Requirements: 9.1.1, 10.1.1_

  - [ ] 10.4 Implement the version-pin monotonicity check (proto + server compat)
    - Inside `ci.rs`, add `fn run_version_pin_monotonicity(base: &dyn ContainerRef<'_>, pin: PinKind) -> Result<CiCheckResult, BuildError>` that (a) resolves the last tag matching `v*` via `git describe --tags --abbrev=0 --match 'v*'`; (b) extracts the named constant (either `TEMPORAL_PROTO_VERSION` or `TEMPORAL_SERVER_COMPAT`) from `crates/tokeira-build-info/src/pinned.rs` at the tip and at the tag; (c) compares via semver (workspace-pinned `semver` dep)
    - Fail the check if the tip version is lower than the tag version AND the tip commit message does NOT contain the matching downgrade override trailer (`Proto-Downgrade:` for proto, `Server-Compat-Downgrade:` for server compat)
    - `PinKind::Proto` and `PinKind::ServerCompat` dispatch to the same logic with different constant names and override trailers
    - THE `CiCheck::BumpTrailer` check is implemented in task 13.18, not here — this task covers only the monotonicity check family
    - _Requirements: 5.4, 10.1.1_

  - [ ]* 10.5 Write a property test for `run_ci_checks` dispatch
    - **Property P-CI-2: CiCheckRequest selection**
    - **Validates: Requirement 10.1.5**
    - Use `MockDaggerClient` (the existing test harness in `crates/tokeira-build/src/testing.rs`). For each `CiCheck` variant and for the all-checks default, assert the expected set of `with_exec` calls is recorded on the mock
    - Test location: `crates/tokeira-build/src/pipelines/ci.rs` `#[cfg(test)]` module
    - _Requirements: 10.1.5_

  - [ ] 10.6 Scaffold `apps/tkr/src/commands/ci/`
    - Create `apps/tkr/src/commands/ci/mod.rs` with a `CiCommand` enum (variant `Check { check: Option<CliCiCheck>, json: bool }`) and a `pub async fn run(command: CiCommand, format: OutputFormat) -> Result<()>` entry point
    - Define `CliCiCheck { NoWallclock, ProtoMonotonicity, ServerCompatMonotonicity, BumpTrailer }` with `clap::ValueEnum` and `From<CliCiCheck> for CiCheck`
    - Add `Ci(CiArgs)` variant to `apps/tkr/src/cli.rs::Command`; wire the dispatcher arm in `apps/tkr/src/main.rs`
    - _Requirements: 10.2_

  - [ ] 10.7 Implement the re-exec path and local invocation
    - In `apps/tkr/src/commands/ci/mod.rs::run`, check `should_reexec_under_dagger()` at entry; when absent, format the argv tail and call the shared `reexec_under_dagger`
    - When session env is present, construct a `CiCheckRequest`, call `run_ci_checks`, render output (human table or JSON via `--json`), exit status 0/1/2 per Req 10.2.6
    - _Requirements: 10.2_

  - [ ]* 10.8 Write integration test for `tkr ci check`
    - Invoke `tkr ci check` via `std::process::Command` against a clean working tree; assert exit 0 and both checks PASSED in the JSON output
    - Introduce a temporary `SystemTime::now()` call in `crates/tokeira-build-info/build.rs`; invoke `tkr ci check no-wallclock`; assert exit 1 and the NoWallclock check FAILED
    - Remove the temporary call; re-run; assert exit 0
    - Test location: `apps/tkr/tests/ci_check.rs`, gated behind `integration-test` feature per AGENTS.md
    - _Requirements: 10.2, 10.3_

- [ ] 11. Source tree hash helper
  - [ ] 11.1 Implement `compute_source_tree_hash` in `tokeira-build`
    - Add `pub fn compute_source_tree_hash(workspace_root: &Path) -> Result<String, BuildError>` to `crates/tokeira-build/src/pipelines/ci.rs` (or a sibling `source_tree_hash.rs` module if it grows)
    - The function walks the workspace applying the exclusion list from Req 1.3.3 (reuses `TOKEIRAD_WORKSPACE_EXCLUDES` as the baseline plus the spec-specific extras), sorts paths deterministically, and returns a lowercase SHA-256 hex string
    - Used by the Dagger build pipeline owned by [`image-lifecycle`](../image-lifecycle/requirements.md) and by `tkr image build` for reproducible-build provenance; operators who want to compute the hash directly can invoke it via a future `tkr provenance source-hash` subcommand (tracked as a non-blocking follow-up)
    - _Requirements: 1.3, 6.1_

  - [ ]* 11.2 Write property test for hash determinism
    - **Property P-CI-1: Source Tree Hash Determinism**
    - **Validates: Requirement 1.3.4**
    - Generate arbitrary file trees via `proptest` (bounded depth, bounded file sizes) in a `tempfile::TempDir`; hash twice; assert byte-equal
    - Shuffle the traversal order in a test-only alternate implementation; assert the sort produces the same hash
    - Test location: `crates/tokeira-build/src/pipelines/ci.rs` `#[cfg(test)]` module
    - _Requirements: 1.3.4_

- [ ] 12. CODEOWNERS and Bump PR 0 baseline
  - [ ] 12.1 Create `.github/CODEOWNERS`
    - Add a line `crates/tokeira-build-info/src/pinned.rs @iw/tokeira-compat-owners` (team handle placeholder — use the concrete maintainer handle that exists on GitHub today; the team handle convention survives Pipeline Foundation wiring later)
    - Add `docs/compat-bumps/ @iw/tokeira-compat-owners` so retroactive and future PR-body records are owned by the same group
    - Document in the file's header comment that this file is informational until `pipeline-foundation` wires branch protection
    - _Requirements: 5.5.6_

  - [ ] 12.2 Author Bump PR 0 baseline record
    - Create `docs/compat-bumps/0-baseline.md` capturing the initial `TEMPORAL_SERVER_COMPAT = "1.27.0"` claim with the full PR body shape Req 5.5.4 mandates, but filled in retroactively
    - Header note: "This is a retroactive baseline per Req 5.5.9. The claim predates the `tkr compat bump` protocol; this document establishes the starting point so future bump PRs have a comparable disposition table to diff against."
    - Include the Upstream Releases table for Temporal server 1.27.0 itself (the single release), the Disposition Table covering upstream surfaces tokeira encountered up to 1.27.0 based on the feature matrix as of this spec's landing, Matrix Delta marked `(baseline — no delta)`, SDK Test-Suite Evidence marked `(baseline — suites not yet running)`, and the reviewer checklist
    - _Requirements: 5.5.9_

  - [ ] 12.3 Update `pinned.rs` rationale comment to cite the baseline
    - The rationale comment above the `TEMPORAL_SERVER_COMPAT` constant MUST point at `docs/compat-bumps/0-baseline.md` with the phrase "Last bump: baseline — see docs/compat-bumps/0-baseline.md"
    - Future bump PRs will amend this comment per Req 5.5.8
    - _Requirements: 5.5.8, 5.5.9_

- [ ] 13. `tkr compat bump` command (Feature 11)
  - [ ] 13.1 Scaffold `crates/tokeira-build/src/compat_bump/`
    - Create the module tree per design.md §8: `mod.rs`, `phases/{preflight,evidence,surfaces,mutate,publish}.rs`, `github.rs`, `template.rs`, `trailer.rs`, `pr_template.md`
    - Define the public surface: `BumpRequest`, `BumpTrigger`, `ResumePolicy`, `BumpOutcome`, `BumpError`, `async fn run_bump(request: BumpRequest) -> Result<BumpOutcome, BumpError>`
    - Re-export the public surface from `crates/tokeira-build/src/lib.rs`
    - _Requirements: 11.1_

  - [ ] 13.2 Add `octocrab`, `semver`, `tinytemplate`, `git2`, `secrecy`, `dirs` workspace deps
    - `octocrab` — workspace-pinned at the current stable
    - `semver` — workspace-pinned; reuse the same version the proto-sync pipeline already uses if applicable
    - `tinytemplate` — workspace-pinned; alternative: hand-rolled binding if the review prefers zero-dep. Design.md §8 recommends `tinytemplate`; defer to implementer judgement during this task
    - `git2` — for `pinned.rs` commit trailer extraction and history walks. Branch creation, commit, push remain shell-outs to `git` per design.md §8
    - `secrecy` + `dirs` — for `SecretString` token handling and config-dir resolution
    - Confirm none of these leak into `tokeira-build-info` (which must stay `[dependencies]`-free; Req 9.2)
    - _Requirements: 11.4.3, 9.2_

  - [ ] 13.3 Implement Phase A (preflight)
    - In `phases/preflight.rs`, implement `async fn execute(ctx: &mut BumpContext) -> Result<(), BumpError>`
    - Steps: read `pinned.rs` via `tokeira-build-info::pinned_source_path(&ctx.workspace_root)`; parse `TEMPORAL_SERVER_COMPAT` via a single-constant text parse; validate target version strictly greater than current; validate working tree clean via `git status --porcelain`; validate current branch matches `ctx.default_branch` (default `main`) via `git symbolic-ref --short HEAD`; call `ctx.github.get_user()` and assert scopes include `public_repo` + `pull_requests: write`
    - Emit `BumpError::AlreadyOnVersion` (exit 0), `BumpError::Downgrade` (exit 1), `BumpError::DirtyWorkingTree`, `BumpError::WrongBranch`, `AuthError::NoToken`, `AuthError::InsufficientScopes` as appropriate
    - _Requirements: 11.2.1 (Phase A), 11.4.1, 11.4.2_

  - [ ]* 13.4 Write unit tests for Phase A
    - Test each preflight failure: empty token, invalid token (mocked octocrab returning 401), missing scopes (mocked 200 with incomplete `X-OAuth-Scopes` header), dirty working tree (via `tempfile` repo with an uncommitted file), wrong branch (via a repo on a non-default branch), target equal to current, target older than current
    - Test location: `crates/tokeira-build/src/compat_bump/phases/preflight.rs` `#[cfg(test)]` module
    - _Requirements: 11.7.6_

  - [ ] 13.5 Implement Phase B (evidence) — release enumeration and matrix delta
    - In `phases/evidence.rs`, implement release enumeration via `octocrab`'s paginated releases endpoint for `temporalio/temporal`
    - Filter releases by tag range: `>current && <=target` under semver
    - For each release in range, fetch the release body; cache under `target/tkr/compat-cache/<tag>`
    - Compute the matrix delta: find the commit referenced by the previous `Server-Compat-Bump:` trailer via `git log --grep='^Server-Compat-Bump:' -1 --format=%H`; if no match, use the Bump PR 0 baseline commit (the commit that added `docs/compat-bumps/0-baseline.md`); compare `FEATURE_MATRIX` `(id, state)` pairs between the two commits using `git show <commit>:crates/tokeira-compatibility/src/matrix.rs`
    - Populate `BumpContext::evidence`
    - _Requirements: 11.2.1 (Phase B), 11.4.4, 11.4.5_

  - [ ]* 13.6 Write unit tests for Phase B evidence gathering
    - Mock `octocrab` to return a canned release list; assert the command fetches the expected range
    - Assert cache reuse: set the cache path; rerun; assert `octocrab` was called zero times on the second run
    - Assert rate-limit handling: mock a 429 response with `X-RateLimit-Reset`; assert the command surfaces `BumpError::RateLimited { reset_at }` with the correct timestamp
    - Test location: `crates/tokeira-build/src/compat_bump/phases/evidence.rs` `#[cfg(test)]` module
    - _Requirements: 11.4.4, 11.7.6_

  - [ ] 13.7 Implement Phase C (mutate) — branch, pinned.rs, commit with trailer, local CI check
    - In `phases/mutate.rs`, create the bump branch via `git switch -c compat/server-compat-bump-<old>-<new>`; honour `ResumePolicy` (`StrictNew` fails if branch exists; `Resume` fast-forwards; `Reset` deletes and recreates)
    - Update `pinned.rs`: bump the `TEMPORAL_SERVER_COMPAT` constant and the rationale comment (placeholder `PR #?`)
    - Write the commit message via `render_commit_message(ctx)`
    - Append the `Server-Compat-Bump:` trailer via `git commit --trailer "..."` (or equivalent through `git interpret-trailers`)
    - Invoke the Feature 10 CI pipeline: `run_ci_checks(&CiCheckRequest { workspace_root, checks: vec![] }, &default_dagger)?`; assert all results pass; if any check fails, surface `BumpError::CiChecksFailed(report)` and leave the branch for debugging
    - _Requirements: 11.2.1 (Phase C), 11.3.1, 11.3.5, 11.6.1, 11.6.2, 11.6.3, 11.8.1_

  - [ ]* 13.8 Write unit tests for Phase C
    - Use `tempfile` to create a tiny git repository with a minimal `pinned.rs` and a matrix stub
    - Test branch creation, pinned.rs rewriting, commit trailer correctness (`BumpTrailer::parse` round-trips), resume/reset semantics
    - Mock the `run_ci_checks` call to return a passing report; assert Phase C succeeds; swap to a failing report; assert Phase C surfaces `BumpError::CiChecksFailed` and the branch remains
    - Test location: `crates/tokeira-build/src/compat_bump/phases/mutate.rs` `#[cfg(test)]` module
    - _Requirements: 11.6, 11.7.6_

  - [ ] 13.9 Implement Phase D (publish) — push, PR open, amend for PR number
    - In `phases/publish.rs`, shell out to `git push -u origin <branch>` for the push; handle non-fast-forward as `BumpError::PushRejected { git_output }`
    - Render PR title and body via `template.rs::render_*`
    - Open the PR via `octocrab::pulls::PullRequestHandler::create`; on 5xx, retry once with exponential backoff; if retry fails, write the rendered PR body to `target/tkr/compat-cache/<branch>-pr-body.md` and surface `BumpError::PrOpenFailed { body_path }`
    - On successful PR creation, rewrite the `pinned.rs` rationale comment to replace `PR #?` with the real PR number, `git commit --amend --no-edit`, and `git push --force-with-lease`
    - If `--no-open` is set, stop after the push and return `BumpOutcome { pr_url: None, ... }`
    - _Requirements: 11.2.1 (Phase D), 11.3.5, 11.4.6_

  - [ ]* 13.10 Write integration tests for Phase D against mocked octocrab
    - Use the `wiremock` crate (or `mockito`) to stand up a mock GitHub endpoint returning canned responses
    - Test: happy path opens a PR and the body matches the rendered template; retry on 5xx; fail after two 5xx with `PrOpenFailed`; rate-limited response surfaces `RateLimited`
    - Test location: `crates/tokeira-build/src/compat_bump/phases/publish.rs` `#[cfg(test)]` module (or a dedicated `tests/phase_d_integration.rs` file if it grows)
    - _Requirements: 11.7.4, 11.7.6_

  - [ ] 13.11 Implement `template.rs` with `tinytemplate` binding
    - Load `pr_template.md` at compile time via `include_str!`
    - Define `TemplateBindings` struct with every placeholder the template consumes; derive `Serialize`
    - Implement `render_pr_body(ctx: &BumpContext) -> Result<String, TemplateError>` and `render_pr_title(ctx: &BumpContext) -> String`
    - _Requirements: 11.3.3, 11.3.4_

  - [ ]* 13.12 Write property test for PR body rendering determinism
    - **Property P-BUMP-1: PR body rendering determinism**
    - **Validates: Requirement 11.7.1**
    - For any valid `BumpContext`, `render_pr_body` twice produces byte-equal output; every placeholder is bound (no `{{ foo }}` leaks); the output contains the `Server-Compat-Bump:` trailer exactly once
    - Test location: `crates/tokeira-build/src/compat_bump/template.rs` `#[cfg(test)]` module
    - _Requirements: 11.7.1, 11.7.2_

  - [ ] 13.13 Implement `trailer.rs` parsing and rendering
    - Define `BumpTrailer { old, new, trigger }` with `parse(&str) -> Result<Self, TrailerError>` and `render(&self) -> String`
    - The regex: `^Server-Compat-Bump: (\d+\.\d+\.\d+) -> (\d+\.\d+\.\d+), trigger: ([123])$`
    - Provide `find_in_commit_message(&str) -> Option<BumpTrailer>` that walks the message and picks the last matching trailer (so trailer-appending preserves correctness)
    - _Requirements: 11.3.1, 5.5.5_

  - [ ]* 13.14 Write property test for trailer round-trip
    - **Property P-BUMP-2: Trailer round-trip**
    - **Validates: Requirements 5.5.5, 11.3.1, 11.7.3**
    - For any `(old, new, trigger)` tuple where old and new are valid semver, `BumpTrailer { ... }.render().parse()` equals the original
    - Negative cases: missing trigger, non-semver versions, wrong arrow (`->`, `-->`), wrong trigger digit (`0`, `4`, `a`); assert each yields `TrailerError`
    - Test location: `crates/tokeira-build/src/compat_bump/trailer.rs` `#[cfg(test)]` module
    - _Requirements: 5.5.5, 11.7.3_

  - [ ] 13.15 Implement `github.rs` — `GithubAuth`, `Octocrab` wrapper, release enumeration, rate-limit handling
    - `GithubAuth::from_env_or_config()` per design.md §8
    - `Octocrab` builder with `User-Agent: tokeira-compat-bump/<version>`
    - Release enumeration: `ctx.github.list_releases_in_range(owner, repo, old, new) -> Result<Vec<ReleaseSummary>, GithubError>`
    - Rate-limit reader: inspect `X-RateLimit-*` response headers; surface as `BumpError::RateLimited { reset_at }`
    - _Requirements: 11.4_

  - [ ] 13.16 Implement the `tkr compat bump` CLI wiring
    - Create `apps/tkr/src/commands/compat/mod.rs` (if not present; otherwise extend) and `apps/tkr/src/commands/compat/bump.rs`
    - Define `BumpArgs` with clap per design.md §8
    - Define `CliTrigger` enum and its `From<CliTrigger> for BumpTrigger` impl
    - `resolve_trigger(arg, yes, json)` prompts interactively when absent and the mode is interactive; fails when absent in `--yes` or `--json`
    - Wire `Compat(CompatArgs)` into `apps/tkr/src/cli.rs::Command` (if not already split) with `CompatCommand { Show, Diff, Bump }`
    - In `apps/tkr/src/main.rs`, dispatch to `commands::compat::bump::run(args, format)`
    - _Requirements: 11.1_

  - [ ]* 13.17 Write CLI parse tests for `tkr compat bump`
    - Parse `tkr compat bump --to 1.29.0`: assert `to == 1.29.0`, `trigger == None`, `dry_run == false`
    - Parse `tkr compat bump --to 1.29.0 --trigger 3 --yes`: assert `trigger == Some(Three)`, `yes == true`
    - Parse `tkr compat bump --to 1.29.0 --resume --reset`: assert clap rejects (conflicts_with)
    - Parse `tkr compat bump --to not-semver`: assert clap or the semver parse returns a usage error
    - Test location: `apps/tkr/src/commands/compat/bump.rs` `#[cfg(test)]` module
    - _Requirements: 11.1, 11.7.3_

  - [ ] 13.18 Wire the `CiCheck::BumpTrailer` check into `run_ci_checks`
    - In `crates/tokeira-build/src/pipelines/ci.rs`, add the `BumpTrailer` and `ServerCompatMonotonicity` variants to the `CiCheck` enum per design.md §7
    - Implement `run_bump_trailer_check` inside the Dagger container: `git log -1 --name-only` to detect `pinned.rs` in the diff; if present, `git log -1 --format=%B | git interpret-trailers --parse` to extract the trailer; invoke `BumpTrailer::parse`; if absent or invalid, fail the check
    - Implement `run_version_pin_monotonicity(PinKind)` that generalises the existing `run_proto_monotonicity` to cover both `TEMPORAL_PROTO_VERSION` and `TEMPORAL_SERVER_COMPAT`
    - _Requirements: 5.5.5, 11.8.2_

  - [ ]* 13.19 Write integration test for the full bump flow
    - Gated behind `integration-test` feature and `#[ignore]`
    - Requires a `GH_TOKEN` with permissions on a fork of the tokeira repository
    - Runs `tkr compat bump --to <next-patch> --trigger 3 --yes` against the fork; asserts a PR is opened; closes the PR via API
    - Test location: `apps/tkr/tests/compat_bump_integration.rs`
    - _Requirements: 11.7.5_

  - [ ] 13.20 Deferred follow-up: `--derive-surfaces` stage 2
    - Implement the full skeleton-table generation per Req 11.5.3 — proto-tree diff classification into new RPCs / new fields / new messages / new enum variants, each mapped to the matching matrix row where one exists
    - Stage 1 ships with the core Feature 11 landing: raw diff as an appendix in the PR body
    - Stage 2 lands after the core is exercised in practice
    - This sub-task SHALL be split into its own commit landing after 13.1–13.19 are stable
    - _Requirements: 11.5.6_

- [ ] 14. Documentation and integration
  - [ ] 14.1 Update `README.md`
    - Add a "Temporal compatibility" section citing `TEMPORAL_SERVER_COMPAT`, `TEMPORAL_PROTO_VERSION`, summarising the feature matrix by state (e.g., "33 implemented, 5 experimental, 2 stubbed, 1 unsupported"), and pointing at `tkr compat show` for detail
    - _Requirements: 9.3.1_

  - [ ] 14.2 Update `AGENTS.md`
    - Add the proto bump workflow (Req 5.2) to "Working Agreements"
    - Add the server-compat independence rule (Req 5.3) to the same section
    - Add a pointer from any "adding a new feature" checklist to this spec's matrix declaration
    - Add `tkr ci check` to the Enforced Commands list (Req 10.3.1) as the pre-push gate for compatibility invariants. Mention that `pipeline-foundation` (backlog P16) will wire the same checks into remote triggers; until then, `tkr ci check` is the canonical local verdict
    - Add a "Server compat bump protocol" subsection summarising Req 5.5: the three triggers, the `tkr compat bump` command, the `Server-Compat-Bump:` trailer requirement, and the CODEOWNERS gate
    - _Requirements: 9.3.2, 10.3, 5.5.10_

  - [ ] 14.3 Add `README.md` to `crates/tokeira-build-info/`
    - Document the env vars a CI or hand-run release build must set (`TOKEIRA_GIT_SHA`, `TOKEIRA_SOURCE_TREE_HASH`, `CI`)
    - Document the debug build fallbacks
    - Document the `pinned.rs` bump workflow — now just a pointer at Feature 5.5 and `tkr compat bump`
    - _Requirements: 6.2.3_

  - [ ] 14.4 Final checkpoint — full workspace verification
    - Run `cargo +nightly fmt --all --check`
    - Run `cargo lint`
    - Run `cargo test-lint`
    - Run `cargo check --workspace`
    - Run `cargo test --workspace`
    - Run `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"`
    - Run `tkr ci check` against a clean working tree (expected: all checks PASSED, exit 0)
    - All commands must pass with zero warnings
