# Requirements Document: Temporal Compatibility

## Introduction

Tokeira is a Temporal-compatible durable execution engine. The public contract is the Temporal gRPC API, the Temporal protobuf types, and the Temporal SDK behaviour expectations. Tokeira is an original Rust implementation that presents the same wire-level surface as the Temporal Go server; it is not a port.

This spec makes compatibility explicit. It governs:

1. Which version of the Temporal server release tokeira claims compatibility with.
2. Which version of the Temporal protobuf definitions tokeira vendors.
3. Which Temporal features tokeira implements, gates behind dynamic config, stubs for wire compatibility, or explicitly does not support.
4. Which language SDK versions tokeira is known to work with.
5. What compatibility metadata gets embedded in the `tokeirad` binary and exposed at runtime.
6. How clients discover all of the above through the standard Temporal capability handshake.

The spec owns the compatibility contract and the build-time metadata. It consumes the image build pipeline from [`image-lifecycle`](../image-lifecycle/requirements.md) (which passes git metadata as environment variables to the build) and informs [`ecs-deployment`](../ecs-deployment/requirements.md) and [`shard-placement-membership`](../shard-placement-membership/requirements.md) for mixed-version detection.

### What this spec delivers

- A new `tokeira-build-info` library crate that exposes compile-time constants for all compatibility metadata.
- A `build.rs` in `tokeira-build-info` that reads values from environment variables, parses `rust-toolchain.toml`, and computes a source-tree hash.
- A canonical `FeatureMatrix` in `tokeira-compatibility` (either a sibling crate or a module in `tokeira-build-info` — design phase decides) enumerating every Temporal feature tokeira exposes, each in one of four states.
- A canonical `SdkMatrix` declaring minimum and maximum tested SDK versions per language.
- A `GetSystemInfo` RPC implementation in `tokeira-edge` that walks the feature matrix and returns the correct capabilities blob to connecting clients.
- A `tkr compat show` CLI subcommand that prints build-time metadata, the feature matrix with runtime state, and the SDK matrix — queryable both for local builds and against a running deployment.
- A Dagger-backed local CI pipeline (Feature 10) invokable via `tkr ci check` that runs the no-wallclock and proto-monotonicity checks against the current workspace. Remote-trigger wiring is deferred to the `pipeline-foundation` spec.
- Property tests enforcing matrix completeness (every Temporal RPC maps to exactly one state), capability-handshake consistency (every `capabilities.*` flag maps to a feature), and proto version monotonicity (bumps never downgrade).

### What this spec does NOT cover

- The implementation of any Temporal feature. Each feature has its own spec.
- Dynamic config itself (how experimental features get toggled at runtime). This spec declares the taxonomy; dynamic config is a separate concern.
- Client-library version negotiation. SDKs negotiate via the capability handshake this spec defines; the SDK-side logic is not our concern.
- Tokeira internal-state backward compatibility (state schemas, storage migration). Out of scope.
- ABI or plugin compatibility. Tokeira does not ship plugin interfaces.
- Remote CI triggers (GitHub Actions, nightly schedules, release pipelines). The Dagger pipeline landed by Feature 10 is the portable local substrate; remote-trigger wiring is owned by the `pipeline-foundation` spec (backlog P16) and will invoke the same `run_ci_checks` function so verdicts do not diverge.

### Cross-references

- [`image-lifecycle`](../image-lifecycle/requirements.md): The Dagger build pipeline passes git metadata (`TOKEIRA_GIT_SHA`, working-tree clean flag) as environment variables to the tokeirad build; `tokeira-build-info` reads them in its `build.rs`. Image-lifecycle's reproducible-build property depends on this spec excluding wall-clock timestamps from the binary. This spec reuses image-lifecycle's `DaggerClient` abstraction and `TOKEIRAD_WORKSPACE_EXCLUDES` for Feature 10.
- [`shard-placement-membership`](../shard-placement-membership/requirements.md): Controller snapshots carry `TOKEIRA_VERSION` per node so the cluster detects mixed-version deployments.
- [`ecs-deployment`](../ecs-deployment/requirements.md): Operators use `tkr exec` or `tkr compat show` against a live cluster to verify every task is on the same version before a rolling deployment proceeds.
- [`tkr-cli`](../tkr-cli/requirements.md): Adds the `compat` and `ci` command groups.
- [`pipeline-foundation`](../pipeline-foundation/requirements.md): Will consume Feature 10's `run_ci_checks` function as the single source of truth for compatibility checks, wiring it into remote triggers (GitHub Actions, nightly, release pipelines). Until `pipeline-foundation` lands, `tkr ci check` is the canonical local pre-push gate.

## Glossary

- **TOKEIRA_VERSION**: The semver version of the `tokeirad` binary, read from `Cargo.toml` at build time (e.g., `0.1.0`). Identifies the tokeira release independently of any Temporal version.
- **TOKEIRA_GIT_SHA**: The short git SHA (7 or 8 hex characters) of the source tree at build time, with a `-dirty` suffix if the working tree has uncommitted changes. Supplied to the build via the `TOKEIRA_GIT_SHA` environment variable (set by the Dagger pipeline or by `cargo build` locally).
- **TEMPORAL_PROTO_VERSION**: The version of the vendored Temporal protobuf definitions — a tagged release of `temporalio/api` (e.g., `v1.47.0`) that tokeira's `tokeira-proto` crate mirrors. Embedded as a `&str` constant.
- **TEMPORAL_SERVER_COMPAT**: The Temporal server release tokeira claims behavioural compatibility with (e.g., `1.27.0`). A _claim_, not a derivation. SDK test suites use this to pick which tests to run; operator-facing diagnostics cite it. Decoupled from TEMPORAL_PROTO_VERSION — they may move independently.
- **RUST_TOOLCHAIN**: The Rust toolchain channel or version resolved from `rust-toolchain.toml` at build time (e.g., `1.95.0`). Embedded for reproducibility audits.
- **SOURCE_TREE_HASH**: A SHA-256 hash of the canonical tokeira source tree, computed with a deterministic file ordering. Excludes `target/`, build artefacts, editor files (`.vscode/`, `.idea/`), and OS junk (`.DS_Store`, `Thumbs.db`). Enables reproducibility audits across hosts.
- **Feature_Matrix**: The canonical enumeration of Temporal features tokeira knows about, each in exactly one `Feature_State`. Defined once; consumed by kernel (compile-time assertions), edge (runtime handler selection), and CLI (`tkr compat show`).
- **Feature_State**: One of four values per feature: `Implemented`, `Experimental`, `Stubbed`, `Unsupported`. Semantics defined in Requirement 2.1.
- **Capability_Handshake**: The response to the Temporal `GetSystemInfo` gRPC that clients call at connection time. Includes `server_version` (string), a `capabilities.*` flags blob, and a tokeira-specific extension field `tokeira_build_info`.
- **SDK_Compat_Entry**: A record per language (Go, TypeScript, Python, Java, .NET) declaring minimum SDK version, maximum tested SDK version, known-incompatible versions with reasons, and a link to the CI test suite.
- **Build_Provenance**: The combination of `TOKEIRA_VERSION`, `TOKEIRA_GIT_SHA`, `TEMPORAL_PROTO_VERSION`, `TEMPORAL_SERVER_COMPAT`, `RUST_TOOLCHAIN`, `SOURCE_TREE_HASH`, and the feature-matrix digest. Logged at `tokeirad` startup; printed by `tokeirad --version --verbose`; included in the capability handshake.

## Requirements

---

## Feature 1: Build-Time Compatibility Metadata

### Requirement 1.1: Metadata crate

**User Story:** As a Tokeira developer, I want a dedicated crate that owns the compatibility metadata constants, so that kernel, edge, runtime, CLI, and test code all read the same values with zero risk of drift.

#### Acceptance Criteria

1. THE workspace SHALL include a new library crate at `crates/tokeira-build-info/` exposing compile-time constants: `TOKEIRA_VERSION: &str`, `TOKEIRA_GIT_SHA: &str`, `TEMPORAL_PROTO_VERSION: &str`, `TEMPORAL_SERVER_COMPAT: &str`, `RUST_TOOLCHAIN: &str`, `SOURCE_TREE_HASH: &str`.
2. THE crate SHALL use a `build.rs` to populate each constant at compile time. The `build.rs` SHALL read from environment variables (`TOKEIRA_GIT_SHA`, `TOKEIRA_SOURCE_TREE_HASH`), from `Cargo.toml` (version), from `rust-toolchain.toml` (toolchain), and from constants in a source file (`TEMPORAL_PROTO_VERSION`, `TEMPORAL_SERVER_COMPAT`).
3. THE crate SHALL have no runtime dependencies beyond `std`. All values are resolved at build time.
4. THE crate SHALL expose a `fn summary() -> BuildInfo` that returns a struct with all fields, for ergonomic runtime use.

### Requirement 1.2: Build provenance is mandatory in release builds

**User Story:** As a Tokeira operator, I want every release binary to carry non-empty provenance, so that a `tokeirad --version` output is always traceable to a specific source tree and toolchain.

#### Acceptance Criteria

1. WHEN `CARGO_PROFILE` is `release`, THE `build.rs` SHALL fail the build if `TOKEIRA_VERSION` resolves to an empty string.
2. WHEN `CARGO_PROFILE` is `release`, THE `build.rs` SHALL fail the build if `TOKEIRA_GIT_SHA` is empty AND the `CI` environment variable is set (i.e., release builds in CI must always have a git SHA).
3. WHEN `CARGO_PROFILE` is `debug`, THE `build.rs` SHALL tolerate an empty `TOKEIRA_GIT_SHA` and substitute the literal string `dev`. This preserves developer workflow in a fresh clone without git.
4. WHEN the working tree has uncommitted changes, THE build pipeline (not the `build.rs`) SHALL set `TOKEIRA_GIT_SHA` to `<sha>-dirty`. The `build.rs` accepts whatever value is provided.
5. THE `build.rs` SHALL fail fast with a descriptive error when `TEMPORAL_PROTO_VERSION` or `TEMPORAL_SERVER_COMPAT` is empty. These are never optional.

### Requirement 1.3: Source tree hash for reproducibility audits

**User Story:** As a Tokeira operator, I want a deterministic hash of the source tree embedded in the binary, so that I can verify two binaries claiming the same version were built from the same tree.

#### Acceptance Criteria

1. THE `tokeira-build-info` crate SHALL expose a `SOURCE_TREE_HASH: &str` constant of exactly 64 lowercase hex characters (SHA-256).
2. THE value SHALL be supplied via the `TOKEIRA_SOURCE_TREE_HASH` environment variable by the build pipeline. THE `build.rs` SHALL NOT compute the hash itself — traversing the source tree at build time violates build hermeticity.
3. THE hash computation (performed by `image-lifecycle`'s Dagger pipeline or a helper binary) SHALL exclude: `target/`, `.git/`, `node_modules/`, `.idea/`, `.vscode/`, `.DS_Store`, `Thumbs.db`, any file matching `*.lock` except `Cargo.lock`, any file matching `.env*`.
4. THE hash computation SHALL use a deterministic file ordering (sorted by relative path, UTF-8-NFC-normalised).
5. WHEN `TOKEIRA_SOURCE_TREE_HASH` is empty in a debug build, THE `build.rs` SHALL substitute a 64-character string of the literal character `0`. This is distinguishable from a real hash (all zeros is statistically impossible) and prevents empty-string ambiguity.

### Requirement 1.4: `--version` and `--version --verbose` output

**User Story:** As a Tokeira operator, I want `tokeirad --version` to print compatibility metadata in a predictable format, so that scripts and logs can parse the output without scraping internals.

#### Acceptance Criteria

1. `tokeirad --version` SHALL print exactly three lines in this format:
   ```
   tokeirad {TOKEIRA_VERSION} ({TOKEIRA_GIT_SHA})
   temporal-server-compat {TEMPORAL_SERVER_COMPAT}
   temporal-proto {TEMPORAL_PROTO_VERSION}
   ```
2. `tokeirad --version --verbose` SHALL print additional lines appended to the short form:
   ```
   rust-toolchain {RUST_TOOLCHAIN}
   source-tree-hash {SOURCE_TREE_HASH}
   feature-matrix-digest {digest}
   sdk-matrix-digest {digest}
   ```
3. `tokeirad --version --json` SHALL print a single JSON object with all fields as keys, matching the short form flat, and feature/sdk matrices as nested objects.
4. FOR ALL invocations of `tokeirad --version` on the same binary, the output SHALL be byte-identical (no timestamps, no locale-dependent formatting).

### Requirement 1.5: Structured startup log entry

**User Story:** As a Tokeira operator, I want the first log entry on `tokeirad` startup to carry full build provenance, so that log aggregators can correlate runtime issues with a specific build.

#### Acceptance Criteria

1. WHEN `tokeirad` starts, it SHALL emit exactly one `tracing::info!` entry at the earliest point after logging initialisation, with event name `tokeirad.startup` and all `Build_Provenance` fields as structured fields.
2. THE startup log SHALL include at minimum: `tokeira_version`, `tokeira_git_sha`, `temporal_server_compat`, `temporal_proto_version`, `rust_toolchain`, `source_tree_hash`, `feature_matrix_digest`, `sdk_matrix_digest`.
3. THE startup log SHALL NOT include any wall-clock timestamp beyond what the tracing subscriber injects on every event. The entry's own payload is free of timestamps (preserving log-line-for-log-line equality across runs).

### Requirement 1.6: Explicit policy on build timestamps

**User Story:** As a Tokeira developer, I want a documented rule that build timestamps are never embedded in the binary, so that reproducibility audits are mechanically verifiable.

#### Acceptance Criteria

1. THE `tokeira-build-info` crate SHALL NOT expose any constant or struct field representing build wall-clock time.
2. THE `build.rs` SHALL NOT read `chrono::Utc::now()`, `std::time::SystemTime::now()`, or any other source of wall-clock time.
3. WHEN an operator needs build wall-clock provenance, they SHALL derive it from git commit timestamps (`git show -s --format=%ct`) or from container registry push metadata. This is a documented operator responsibility; tokeira does not embed it.
4. THE reproducibility property owned by [`image-lifecycle`](../image-lifecycle/requirements.md) SHALL pass byte-for-byte equality of the application binary layer under this rule.

---

## Feature 2: Feature Matrix

### Requirement 2.1: Feature state taxonomy

**User Story:** As a Tokeira client author, I want four well-defined feature states so that client behaviour under each state is predictable.

#### Acceptance Criteria

1. THE `Feature_State` enum SHALL have exactly four variants: `Implemented`, `Experimental`, `Stubbed`, `Unsupported`. No other states.
2. **Implemented**: The feature's RPC(s) are fully supported. Requests succeed on the happy path, fail with feature-specific error codes on invalid input. Corresponding `capabilities.*` flag in the handshake is `true`.
3. **Experimental**: The feature is compiled in but disabled by default. When the operator's dynamic config enables the feature (globally or per-namespace), it behaves as `Implemented`. When disabled, the RPC SHALL return `FailedPrecondition` with `details` naming the feature and pointing at the dynamic-config key that enables it. Corresponding `capabilities.*` flag in the handshake reflects the current dynamic-config state at the time of the handshake.
4. **Stubbed**: The RPC is accepted for wire compatibility but always returns `Unimplemented` with `details = "tokeira does not implement feature '<name>' (stub)"`. Clients that treat `Unimplemented` as a graceful degradation signal (the Temporal SDK behaviour) adapt automatically. Corresponding `capabilities.*` flag is `false`.
5. **Unsupported**: The RPC is rejected with `Unimplemented` and `details = "tokeira does not support feature '<name>' (out of scope)"`. The difference from `Stubbed` is operator/roadmap expectation: `Stubbed` means "planned, not yet done"; `Unsupported` means "explicit decision not to support". Corresponding `capabilities.*` flag is `false`.
6. THE handshake response SHALL include a separate `tokeira_feature_states` extension field (non-standard) mapping each feature name to its state string (`implemented`, `experimental`, `stubbed`, `unsupported`). Standard-SDK clients ignore this extension; tokeira-aware tooling reads it.

### Requirement 2.2: Matrix is the single source of truth

**User Story:** As a Tokeira maintainer, I want the feature matrix declared in exactly one place, so that adding a new feature or bumping a state is a single-file change.

#### Acceptance Criteria

1. THE `Feature_Matrix` SHALL be declared as a `const fn FEATURE_MATRIX: &[(FeatureId, FeatureState, &'static str)]` — or a structurally equivalent `const` slice — in `tokeira-build-info` (or a co-located `tokeira-compatibility` sibling crate; design phase decides).
2. THE matrix declaration SHALL be the only place feature states are written in the tokeira codebase.
3. THE matrix SHALL expose a digest: a deterministic hash of the `(feature_id, state)` pairs, computed at compile time via a proc-macro or a const hash function. Digest changes whenever any feature's state changes; used by the startup log and capability handshake as a quick compatibility signal.
4. THE matrix SHALL be exported with `pub use` for external (test and CLI) access, but direct field mutation SHALL be forbidden by the visibility rules of the crate.

### Requirement 2.3: Matrix completeness property

**User Story:** As a Tokeira maintainer, I want every RPC in the Temporal proto to map to exactly one feature in the matrix, so that no RPC can accidentally ship without a documented state.

#### Acceptance Criteria

1. THE matrix SHALL cover every `WorkflowService` and `OperatorService` RPC defined in the vendored Temporal proto set.
2. A property test SHALL enumerate every RPC name in `temporal.api.workflowservice.v1.WorkflowService` and `temporal.api.operatorservice.v1.OperatorService` (via codegen reflection or a generated RPC-name list) and assert that every name has exactly one matching `FeatureId` in the matrix.
3. THE property test SHALL fail the build when a new RPC is introduced in a proto bump without being classified in the matrix.
4. THE property test SHALL fail the build when a feature is removed from the proto but left in the matrix (dead entries).
5. THE property test SHALL run as part of the default `cargo test` invocation.

### Requirement 2.4: Compile-time gates in the kernel

**User Story:** As a Tokeira kernel author, I want the kernel to refuse to compile code paths behind features that are `Stubbed` or `Unsupported`, so that the kernel stays minimal and no one accidentally implements an out-of-scope feature.

#### Acceptance Criteria

1. THE kernel SHALL expose a `cfg_feature!(name)` macro (or an equivalent const-generic approach) that emits the gated code only when the named feature is `Implemented` or `Experimental`.
2. WHEN a kernel module references a `cfg_feature!` name that resolves to `Stubbed` or `Unsupported`, compilation SHALL fail with a message citing the feature name and state.
3. THE macro SHALL NOT rely on cargo features (which are additive and can drift per-dependency). It SHALL resolve against the matrix at compile time.

### Requirement 2.5: Runtime handler selection in the edge

**User Story:** As a Tokeira edge author, I want RPC handlers to dispatch based on feature state uniformly, so that every RPC has consistent behaviour regardless of feature class.

#### Acceptance Criteria

1. THE edge SHALL implement a `dispatch_rpc<F: Feature>(ctx, request)` helper that consults the matrix:
   - `Implemented`: calls the real handler.
   - `Experimental`: checks dynamic config; if enabled, calls the real handler; otherwise returns `FailedPrecondition` per Req 2.1.3.
   - `Stubbed`: returns `Unimplemented` per Req 2.1.4.
   - `Unsupported`: returns `Unimplemented` per Req 2.1.5.
2. THE edge SHALL NOT have per-feature conditional logic outside `dispatch_rpc`. This centralises the feature-state policy.
3. THE dispatch helper SHALL emit a metric per call labelled by feature name and state, so operators can see which stubbed RPCs are being called by which SDKs.

### Requirement 2.6: Initial feature seed

**User Story:** As a Tokeira maintainer, I want an initial seed of feature classifications committed with this spec, so that implementation can start without bikeshedding every feature individually.

#### Acceptance Criteria

1. THE initial matrix SHALL classify at minimum the following features. The design phase refines exact states:

   | Feature | Initial state (design may revise) |
   |---|---|
   | Workflow namespaces | Implemented |
   | Workflow queries | Implemented |
   | Workflow signals | Implemented |
   | Workflow updates | Experimental |
   | Child workflows | Implemented |
   | Cron workflows | Implemented |
   | Continue-as-new | Implemented |
   | Schedules | Experimental |
   | Nexus | Unsupported |
   | Replication / multi-cluster | Unsupported |
   | Archival | Unsupported |
   | Task queue partitions (reachability) | Experimental |
   | Eager workflow start | Implemented |
   | Sticky queues | Implemented |
   | Reset workflow history | Experimental |

2. Every transition between states SHALL require a spec update (design and tasks) and a property-test update — no silent state changes.
3. THE initial matrix SHALL be fully populated for every Temporal RPC covered by Req 2.3.1. Gaps are errors, not defaults.

---

## Feature 3: SDK Compatibility Matrix

### Requirement 3.1: SDK matrix structure

**User Story:** As a Tokeira operator, I want a single declared matrix of supported SDK versions per language, so that I know which client libraries will work.

#### Acceptance Criteria

1. THE `SdkMatrix` SHALL be declared in `tokeira-build-info` (or the `tokeira-compatibility` sibling crate) as a `const` slice of `SdkCompatEntry { language, min_version, max_tested_version, known_incompatible: &[IncompatibleVersion], test_suite_ref }`.
2. THE languages covered in the initial matrix SHALL be: Go, TypeScript, Python, Java, .NET.
3. EACH `IncompatibleVersion` SHALL include the version string, a human-readable reason, and a link to the tracking issue.
4. `test_suite_ref` SHALL be a path or identifier pointing at the CI job that runs the SDK's canonical integration suite against tokeirad.

### Requirement 3.2: Matrix digest and startup log

**User Story:** As a Tokeira operator, I want a digest of the SDK matrix in the startup log, so that I can detect silent matrix changes across rolling deployments.

#### Acceptance Criteria

1. THE `SdkMatrix` SHALL expose a `DIGEST: &str` constant computed at compile time over the `(language, min_version, max_tested_version)` tuples. Excluded from the digest: `known_incompatible` details (frequently updated with new reasons) and `test_suite_ref` (non-semantic).
2. THE digest SHALL be emitted in the startup log (Req 1.5.2).
3. THE digest SHALL be emitted in `--version --verbose` output (Req 1.4.2).

### Requirement 3.3: Round-trip and monotonicity properties

**User Story:** As a Tokeira maintainer, I want property tests enforcing matrix invariants, so that accidental drift is caught in CI.

#### Acceptance Criteria

1. THE SDK matrix property test SHALL assert: for every entry, `min_version <= max_tested_version` under semver comparison.
2. THE property test SHALL assert: no `IncompatibleVersion` entry has a version equal to `max_tested_version` (they are mutually exclusive claims).
3. THE property test SHALL assert: serialising the matrix to JSON and re-parsing yields a structurally equal matrix (round-trip).
4. WHEN a tokeira release bumps `min_version` for a language, a test or PR check SHALL flag the change as a potentially breaking compatibility bump requiring deliberate release notes. This is a process requirement; implementation may be as simple as a CODEOWNERS review gate.

---

## Feature 4: Capability Handshake on `GetSystemInfo`

### Requirement 4.1: `GetSystemInfo` RPC surface

**User Story:** As a Temporal SDK author, I want tokeira to respond to `GetSystemInfo` with an accurate and stable capabilities blob, so that my SDK can enable or disable features automatically.

#### Acceptance Criteria

1. THE edge SHALL implement the `GetSystemInfo` RPC defined in `temporal.api.workflowservice.v1` exactly as specified by the vendored proto version.
2. THE response SHALL set `server_version = TEMPORAL_SERVER_COMPAT`.
3. THE response SHALL populate `capabilities.*` fields by consulting the feature matrix. For each capability flag, the flag is `true` iff the owning feature is `Implemented` OR (`Experimental` AND enabled by dynamic config for the current request's namespace-or-global scope).
4. THE response SHALL include a non-standard extension field `tokeira_build_info` carrying `TOKEIRA_VERSION`, `TOKEIRA_GIT_SHA`, `TEMPORAL_PROTO_VERSION`, `SOURCE_TREE_HASH`, `feature_matrix_digest`, and `sdk_matrix_digest`. Clients that do not understand the extension SHALL ignore it without error.
5. THE response SHALL include a non-standard extension field `tokeira_feature_states` per Req 2.1.6.
6. THE RPC SHALL be callable without authentication, matching Temporal's default (operators that gate it do so via a separate network-level control).

### Requirement 4.2: Handshake consistency property

**User Story:** As a Tokeira maintainer, I want every `capabilities.*` flag in the handshake to map to a feature in the matrix, so that we can never ship a capability that is not backed by a classified feature.

#### Acceptance Criteria

1. A property test SHALL enumerate every field in `temporal.api.workflowservice.v1.GetSystemInfoResponse.Capabilities` and assert that a matching feature exists in the matrix.
2. THE property test SHALL fail the build when a new capability is introduced in a proto bump without being classified.
3. A separate property test SHALL verify that for every feature, the `capabilities.*` flag it owns is `true` iff the feature is `Implemented` in the matrix (the default dynamic-config state for `Experimental` is `disabled`, so the handshake reports `false` — this is the deterministic baseline). The dynamic-config-aware path is tested separately via integration tests.

### Requirement 4.3: Handshake is the authoritative compatibility contract

**User Story:** As a Tokeira operator, I want `GetSystemInfo` to be the single public surface for compatibility discovery, so that SDKs, monitoring, and operator tooling all read the same source.

#### Acceptance Criteria

1. THE spec SHALL document `GetSystemInfo` as the authoritative compatibility surface. No other RPC exposes per-feature state.
2. `tkr compat show` (Feature 7) SHALL, when pointed at a remote deployment, read the same data via `GetSystemInfo` rather than via a tokeira-specific RPC.
3. WHEN a client needs to detect mixed-version deployments, it SHALL call `GetSystemInfo` repeatedly and compare `tokeira_build_info.feature_matrix_digest` across responses.

---

## Feature 5: Proto Version Sync Policy

### Requirement 5.1: Proto version pinning

**User Story:** As a Tokeira maintainer, I want the Temporal proto version pinned in one place, so that all generated code and references use the same version.

#### Acceptance Criteria

1. THE Temporal proto version SHALL be pinned in `tokeira-build-info` as `TEMPORAL_PROTO_VERSION: &str` matching a tagged release of `temporalio/api`.
2. THE pin SHALL correspond to a concrete commit of the vendored proto set at `tokeira-proto/proto/` (or the submodule path, whichever the design phase chooses).
3. WHEN the pin is bumped, all regenerated Rust types in `tokeira-proto` SHALL be committed in the same change set. The build SHALL fail if the version constant and the generated code disagree.

### Requirement 5.2: Proto bump workflow

**User Story:** As a Tokeira maintainer, I want a documented PR workflow for bumping the Temporal proto version, so that upstream changes are reviewed systematically.

#### Acceptance Criteria

1. A proto version bump PR SHALL include: the new version string, regenerated Rust types, an updated feature matrix covering any new RPCs or capability fields introduced by the upstream bump, and a passing build of the matrix completeness property test.
2. A proto version bump PR that removes any previously-defined RPC SHALL be flagged for explicit maintainer review because it is a breaking wire change.
3. A proto version bump PR SHALL NOT require a `TEMPORAL_SERVER_COMPAT` bump. The two values move independently: proto is wire compat; server compat is behavioural compat.

### Requirement 5.3: Proto vs server compat decoupling

**User Story:** As a Tokeira maintainer, I want proto version and server compat alias managed independently, so that we can track upstream wire-format changes without implicitly claiming server behavioural parity.

#### Acceptance Criteria

1. THE `TEMPORAL_PROTO_VERSION` and `TEMPORAL_SERVER_COMPAT` constants SHALL be independent values, declared side-by-side in a single source file but not derived from each other.
2. THE spec SHALL document: proto version is the wire-format contract; server compat is the behavioural claim. A tokeira release may ship with proto `v1.47.0` but server compat `1.27.0` if the upstream proto has advanced faster than tokeira's behavioural coverage.
3. THE startup log (Req 1.5) and capability handshake (Req 4.1) SHALL expose both values separately.

### Requirement 5.4: Proto version monotonicity

**User Story:** As a Tokeira operator, I want the proto version to only go forward across tokeira releases, so that rollbacks don't introduce protocol-level regressions silently.

#### Acceptance Criteria

1. A Dagger-backed local CI check, invoked via `tkr ci check` per Feature 10, SHALL compare the `TEMPORAL_PROTO_VERSION` between the tip commit and the last tagged tokeira release.
2. IF the new version is older (semver-less), the check SHALL fail, requiring an explicit override commit message (`Proto-Downgrade: <reason>`). Downgrades are possible but never accidental.
3. A `TEMPORAL_SERVER_COMPAT` downgrade SHALL similarly require an explicit override (`Server-Compat-Downgrade: <reason>`).
4. GitHub Actions or other remote CI trigger wiring is OUT OF SCOPE for this spec. The Dagger pipeline underlying `tkr ci check` is the portable substrate; remote-trigger wiring is owned by the `pipeline-foundation` spec (backlog P16). Operators MUST run `tkr ci check` before push until `pipeline-foundation` lands; the pre-push step is documented in `AGENTS.md` per Req 9.3.

### Requirement 5.5: Server compat bump protocol

**User Story:** As a Tokeira maintainer, I want a documented, tool-driven protocol for bumping `TEMPORAL_SERVER_COMPAT`, so that the claim stays close to the upstream Temporal tip, matches a written audit trail, survives reviewer turnover, and is never blocked by feature-coverage gaps that the matrix already communicates honestly.

#### Acceptance Criteria

1. THE bump protocol SHALL recognise three bump triggers, any of which is sufficient to initiate a bump:
   - **Trigger 1**: Upstream adds behaviour tokeira already classifies `Implemented` or `Experimental` in the feature matrix. Typically a low-risk paperwork bump.
   - **Trigger 2**: A matrix row moved to `Implemented` and doing so unblocks a claim on an upstream release that introduced the feature. The row move lands first, the bump follows (Req 5.5.4).
   - **Trigger 3**: Calendar drift. Upstream is ≥6 releases ahead of tokeira's current claim for ≥3 months AND the upstream delta is entirely `Stubbed`/`Unsupported` features tokeira's matrix already documents. Trigger 3 is valid AND expected: it keeps the operator-visible claim close to upstream despite partial coverage, because the feature matrix (not `TEMPORAL_SERVER_COMPAT`) is the authoritative source of what tokeira actually does.
2. BUMPS SHALL be driven exclusively by the `tkr compat bump --to <version>` CLI command per Feature 11. Hand-editing `crates/tokeira-build-info/src/pinned.rs` outside this command is a protocol violation; `tkr ci check` SHALL fail when `pinned.rs` changes in a commit whose message lacks the required `Server-Compat-Bump:` trailer (Req 5.5.5).
3. A Server Compat Bump PR SHALL modify `crates/tokeira-build-info/src/pinned.rs` and its rationale comment, and no other file. Feature-matrix row changes and server-compat bumps SHALL NOT appear in the same PR.
4. A Server Compat Bump PR body SHALL include:
   - The chosen trigger (1, 2, or 3) with a one-sentence justification.
   - The Upstream Releases table: one row per Temporal release between the old and new claim, with a link to the upstream release notes and a one-line verbatim quote from those notes.
   - The Disposition Table: every upstream-introduced surface (new RPC, new field, new default, new message, new enum variant) between the old and new claim, each mapped to exactly one tokeira disposition (`Implemented`, `Experimental`, `Stubbed`, `Unsupported`, `Not wire-visible (internal)`) with the matching feature-matrix row where one exists.
   - The Matrix Delta section: every feature-matrix row whose state changed since the previous `TEMPORAL_SERVER_COMPAT` commit. Trigger 1 bumps typically have an empty delta; trigger 2 bumps name the row that unblocked the bump; trigger 3 bumps typically have an empty delta and the PR body says so explicitly.
   - The SDK Test-Suite Evidence section. Where `SdkMatrix.test_suite_ref` entries point at real suites that have been run against tokeirad at the new claim, the section SHALL cite the run. Where the suites are not yet running, the section SHALL say so and SHALL link the tracking issue. A bump SHALL NOT be blocked on the suites being absent (Req 5.5.7).
5. EVERY commit landing a Server Compat Bump SHALL include a trailer line matching the regex `^Server-Compat-Bump: \d+\.\d+\.\d+ -> \d+\.\d+\.\d+, trigger: [123]$`. THE trailer's old version SHALL match `TEMPORAL_SERVER_COMPAT` at the commit's parent; THE trailer's new version SHALL match `TEMPORAL_SERVER_COMPAT` at the commit. `tkr ci check` enforces both invariants mechanically.
6. A Server Compat Bump PR SHALL be approved by a `CODEOWNERS`-named reviewer before merge. THE `CODEOWNERS` file SHALL name `crates/tokeira-build-info/src/pinned.rs` as requiring an owner-team approval; Feature 10 covers the file's creation and wiring. Until GHA branch protection is wired by `pipeline-foundation`, the `CODEOWNERS` file is an informational record enforced by reviewer discipline and by the PR template's reviewer checklist.
7. A Server Compat Bump PR SHALL NOT be blocked on 100% feature coverage of the claimed version. THE server-compat claim is deliberately permitted to run ahead of coverage; the authoritative record of what tokeira actually does is the feature matrix plus `tokeira_feature_states` in the capability handshake.
8. THE `crates/tokeira-build-info/src/pinned.rs` rationale comment SHALL name the latest Server Compat Bump PR number and the trigger. Bump PRs SHALL amend this comment as part of the same commit.
9. THE initial `TEMPORAL_SERVER_COMPAT = "1.27.0"` value predates this protocol. Landing Req 5.5 SHALL include a retroactive "Bump PR 0" task (tracked in tasks.md) that authors a fully-templated PR body for the current `1.27.0` value, establishing the baseline disposition table so future diffs have a starting point. Bump PR 0 MAY be a committed markdown file under `docs/compat-bumps/0-baseline.md` rather than an actual GitHub PR, because the value already exists in the tree; this exception SHALL be documented in the file itself.
10. THE `AGENTS.md` "Working Agreements" section SHALL carry a "Server compat bump protocol" subsection summarising the triggers, the `tkr compat bump` command, the `Server-Compat-Bump:` trailer requirement, and the CODEOWNERS gate.

---

## Feature 6: Build Provenance

### Requirement 6.1: Provenance supplied by `image-lifecycle`

**User Story:** As a Tokeira developer, I want the image build pipeline to supply git metadata to the tokeirad build, so that release images carry valid provenance.

#### Acceptance Criteria

1. THE Dagger pipeline owned by [`image-lifecycle`](../image-lifecycle/requirements.md) SHALL set the environment variables `TOKEIRA_GIT_SHA` and `TOKEIRA_SOURCE_TREE_HASH` on the build container before invoking `cargo build`.
2. THE Dagger pipeline SHALL compute `TOKEIRA_GIT_SHA` using `git rev-parse --short=8 HEAD`, appending `-dirty` if `git status --porcelain` is non-empty.
3. THE Dagger pipeline SHALL compute `TOKEIRA_SOURCE_TREE_HASH` per Req 1.3.3 and Req 1.3.4.
4. THE Dagger pipeline SHALL fail if `TOKEIRA_GIT_SHA` cannot be resolved (e.g., the build is running outside a git checkout and no override is provided).

### Requirement 6.2: Local developer workflow

**User Story:** As a Tokeira developer, I want `cargo build` in a fresh clone without git to still succeed for debug builds, so that the developer loop is not blocked by CI-only policy.

#### Acceptance Criteria

1. WHEN a developer runs `cargo build` (debug) without setting environment variables, THE `build.rs` SHALL invoke `git rev-parse --short=8 HEAD` directly, and if that fails (no `.git`, no `git`), SHALL substitute the literal `dev`.
2. WHEN a developer runs `cargo build --release` without `TOKEIRA_GIT_SHA` set and not in CI (no `CI` env var), THE `build.rs` SHALL warn via `cargo::warning=` but SHALL NOT fail. Release builds outside CI carry provenance `dev` and that is the developer's responsibility to notice.
3. THE `tokeira-build-info` crate README SHALL document the exact environment variables a CI or hand-run release build must set.

### Requirement 6.3: Provenance is never truncated in logs

**User Story:** As a Tokeira operator debugging a production issue, I want the full provenance to appear in every log entry that carries build-info fields, so that log scraping never loses precision.

#### Acceptance Criteria

1. THE startup log (Req 1.5) SHALL emit every provenance field at full length, never truncated.
2. THE capability handshake (Req 4.1.4) SHALL emit every provenance field at full length.
3. `tokeirad --version` SHALL emit full-length values with no ellipsis.

---

## Feature 7: `tkr compat show` CLI

### Requirement 7.1: Subcommand surface

**User Story:** As a Tokeira operator, I want a single command to print all compatibility metadata, so that I can answer "what does this build support" without running the server.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL expose a top-level `compat` subcommand with children: `show`, `diff`.
2. `tkr compat show` SHALL print:
   - Build-time metadata (Feature 1): version, git SHA, server compat, proto version, Rust toolchain, source tree hash, feature matrix digest, SDK matrix digest.
   - Feature matrix: each feature name, state, and (if applicable) the dynamic-config key that enables it.
   - SDK compatibility matrix: each language, min version, max tested version, known-incompatible versions.
3. `tkr compat show --json` SHALL emit the same data as a single JSON object.
4. `tkr compat show --remote <endpoint>` SHALL dial the gRPC endpoint, invoke `GetSystemInfo`, and print the response formatted identically to the local form. Dynamic-config-dependent `Experimental` state SHALL reflect the remote server's current state, not the local build's static classification.

### Requirement 7.2: Local vs remote consistency

**User Story:** As a Tokeira operator, I want `tkr compat show` (local) and `tkr compat show --remote` against the same tokeirad deployment to produce identical output for static fields, so that I can detect drift between the CLI binary and the server binary.

#### Acceptance Criteria

1. WHEN `tkr compat show` (local) and `tkr compat show --remote <endpoint>` are run against the same tokeirad build, the static fields (build-time metadata, feature matrix digest, SDK matrix digest) SHALL be byte-for-byte identical.
2. WHEN the CLI and the server are built from different source trees, the static fields WILL differ — `tkr compat diff` (Req 7.3) highlights the differences.

### Requirement 7.3: `tkr compat diff` subcommand

**User Story:** As a Tokeira operator, I want to diff two tokeirad deployments' compatibility surfaces, so that I can detect version skew in a cluster before scaling operations that assume uniformity.

#### Acceptance Criteria

1. `tkr compat diff --a <endpoint-a> --b <endpoint-b>` SHALL invoke `GetSystemInfo` on both endpoints and print a unified diff of: build-time metadata, feature matrix, SDK matrix.
2. `tkr compat diff` SHALL exit with status 0 if the two endpoints are fully identical on the compared fields, status 1 if any field differs.
3. `tkr compat diff --local <endpoint>` SHALL compare the local CLI build's compatibility to a remote deployment, useful before an operator's `tkr infra apply` that expects a specific tokeirad version.

---

## Feature 8: Correctness Properties

### Requirement 8.1: Matrix completeness property

**User Story:** As a Tokeira maintainer, I want the matrix to enumerate every Temporal RPC, so that no RPC can ship unclassified.

#### Acceptance Criteria

1. A property test SHALL enumerate every RPC name in the vendored Temporal proto (via generated code introspection or a generated RPC-name list) and assert that the feature matrix contains a classification for each. Covered by Req 2.3.
2. THE test SHALL run as part of the default `cargo test` invocation.
3. THE test SHALL use `proptest` or a generative approach only where it adds value; a deterministic assertion over the full RPC list is acceptable and preferred when the RPC count is bounded.

### Requirement 8.2: Capability handshake consistency property

**User Story:** As a Tokeira maintainer, I want every `capabilities.*` flag to trace back to a feature, so that handshake responses can never contain phantom capabilities.

#### Acceptance Criteria

1. A property test SHALL enumerate every field in `GetSystemInfoResponse.Capabilities` and assert that a matching feature exists in the matrix. Covered by Req 4.2.
2. A separate property test SHALL verify: for every feature in the matrix, its capability-flag-value matches the feature's state at default dynamic-config (Req 4.2.3).

### Requirement 8.3: Proto version monotonicity property

**User Story:** As a Tokeira maintainer, I want accidental proto version downgrades to be caught locally before push, so that production binaries never silently revert wire protocol.

#### Acceptance Criteria

1. Covered by Req 5.4. A Dagger-backed local check (not a unit test; runs via `tkr ci check` per Feature 10) SHALL compare `TEMPORAL_PROTO_VERSION` across tagged commits and fail on downgrade unless an explicit override commit message is present.
2. An analogous check SHALL apply to `TEMPORAL_SERVER_COMPAT`.
3. Remote-trigger wiring (GitHub Actions, nightly scheduled runs, release pipelines) is OUT OF SCOPE for this spec per Req 5.4.4.

### Requirement 8.4: SDK matrix round-trip property

**User Story:** As a Tokeira maintainer, I want the SDK matrix JSON round-trip to be property-tested, so that refactors to the matrix struct don't accidentally change the wire shape operators parse.

#### Acceptance Criteria

1. Covered by Req 3.3. A property test SHALL serialise the `SdkMatrix` to JSON and re-parse, asserting structural equality.
2. THE property test SHALL also assert the digest is stable across the round-trip.

### Requirement 8.5: Build info deterministic output property

**User Story:** As a Tokeira developer, I want `tokeirad --version` output to be deterministic for a given build, so that scripts parsing the output never see surprise variance.

#### Acceptance Criteria

1. A unit test SHALL invoke the `--version` formatter with the same `BuildInfo` twice and assert byte-equal output.
2. A unit test SHALL invoke the `--version --json` formatter with the same `BuildInfo` twice and assert byte-equal output.
3. THE tests SHALL NOT execute a real `tokeirad` binary; they SHALL call the formatter functions directly, substituting a canonical `BuildInfo`.

---

## Feature 9: Cross-Cutting Requirements

### Requirement 9.1: No wall-clock in binary

**User Story:** As a Tokeira maintainer, I want no part of the build to embed wall-clock time, so that reproducible builds are mechanically verifiable.

#### Acceptance Criteria

1. Covered by Req 1.6. A Dagger-backed local check (invoked via `tkr ci check` per Feature 10) SHALL scan `tokeira-build-info/build.rs` and the generated `build_info.rs` for references to `SystemTime`, `Utc::now`, `Local::now`, `OffsetDateTime::now_utc`, or other wall-clock calls. Any hit fails the check.
2. THE check SHALL NOT flag `std::time::Instant` usage (monotonic, not wall-clock; not embedded anyway).
3. Remote-trigger wiring (GitHub Actions, nightly scheduled runs) is OUT OF SCOPE for this spec. The Dagger pipeline underlying the check is the portable substrate; remote wiring is owned by the `pipeline-foundation` spec.

### Requirement 9.2: Zero runtime deps for `tokeira-build-info`

**User Story:** As a Tokeira maintainer, I want `tokeira-build-info` to be trivial to depend on, so that every crate (including the kernel's purity-constrained one) can consume it without pulling transitive dependencies.

#### Acceptance Criteria

1. THE `tokeira-build-info` crate SHALL have no runtime dependencies in `[dependencies]` — only `[build-dependencies]`. Its runtime API is pure constants and a small `BuildInfo` struct defined in `std`.
2. THE crate SHALL be `no_std`-compatible where practical; if `no_std` conflicts with the `build.rs` ergonomics (e.g., `String` in `BuildInfo`), the runtime types SHALL use `&'static str` to stay no-alloc.
3. `tokeira-kernel` SHALL be allowed to depend on `tokeira-build-info` without violating the kernel's purity rules (the crate has no I/O, no async, no storage).

### Requirement 9.3: Documentation

**User Story:** As a Tokeira operator new to the project, I want the compatibility story documented in `README.md` and `AGENTS.md`, so that I can understand which Temporal behaviour tokeira claims without reading the spec.

#### Acceptance Criteria

1. THE root `README.md` SHALL include a "Temporal compatibility" section citing `TEMPORAL_SERVER_COMPAT`, `TEMPORAL_PROTO_VERSION`, a summary of the feature matrix by state, and a pointer at `tkr compat show` for full detail.
2. THE root `AGENTS.md` SHALL reference the compatibility ordering rules from Feature 5 (proto bump workflow, server compat independence) and the pre-push `tkr ci check` gate from Feature 10.3.
3. THE `tkr compat show --help` output SHALL be sufficient to understand the command without reading the spec.
4. THE `tkr ci check --help` output SHALL be sufficient to understand the command without reading the spec.
---

## Feature 10: Local CI via Dagger

### Requirement 10.1: Dagger pipeline for compatibility checks

**User Story:** As a Tokeira developer, I want to run every compatibility CI check on my laptop with a single command before I push, so that I get the same verdict a remote CI runner would eventually give — without waiting for a remote job.

#### Acceptance Criteria

1. THE spec SHALL deliver a Dagger-backed local CI pipeline in `crates/tokeira-build/src/pipelines/ci.rs` that runs two checks:
   - **No-wallclock**: the `rg` invariant in Req 9.1.1 executed inside a deterministic container against the current workspace.
   - **Proto monotonicity**: the semver comparison in Req 5.4 executed against `crates/tokeira-build-info/src/pinned.rs` versus the last tagged release.
2. THE pipeline SHALL be invocable from Rust via a public function `pub fn run_ci_checks(request: &CiCheckRequest, dagger: &dyn DaggerClient) -> Result<CiCheckReport, BuildError>` that returns a structured report enumerating each check and its outcome.
3. THE pipeline SHALL mount the workspace into the Dagger container using the exclude list established by `TOKEIRAD_WORKSPACE_EXCLUDES` (or a CI-specific subset thereof), so cold invocations do NOT upload the multi-GB `target/` tree — the invariant proven by the `tkr image build` work landed in commit `3082176`.
4. THE pipeline SHALL use a small `debian:bookworm-slim` or equivalent image with `ripgrep` and `git` installed; it SHALL NOT depend on the full `tokeirad` build image.
5. Individual check selection SHALL be supported via `CiCheckRequest { checks: Vec<CiCheck> }` where `CiCheck { NoWallclock, ProtoMonotonicity, ServerCompatMonotonicity, BumpTrailer }` is an enum; an empty `checks` vector means "run all checks".

### Requirement 10.2: `tkr ci check` command

**User Story:** As a Tokeira developer, I want a `tkr` subcommand that runs the Dagger pipeline with the same re-exec ergonomics as `tkr image build`, so that I don't have to remember `dagger run -- tkr ci check`.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL expose a top-level `ci` command group with children: `check`.
2. `tkr ci check` SHALL run all checks defined in Req 10.1.1 and print a human-readable summary.
3. `tkr ci check <check-name>` SHALL run exactly one check and print the same summary format.
4. `tkr ci check --json` SHALL emit the `CiCheckReport` as a single JSON object on stdout.
5. WHEN `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN` are absent from the environment, `tkr ci check` SHALL re-execute itself under `dagger run` using the re-exec pattern established in `apps/tkr/src/commands/image/mod.rs`. The operator does not need to remember `dagger run`.
6. Exit status SHALL be 0 if all checks pass, 1 if any check fails, 2 on usage error.

### Requirement 10.3: Pre-push operator workflow

**User Story:** As a Tokeira maintainer, I want the spec to document `tkr ci check` as the pre-push gate, so that contributors know the command is the canonical verdict before remote triggers exist.

#### Acceptance Criteria

1. `AGENTS.md` SHALL include `tkr ci check` in the enforced-commands list (alongside `cargo +nightly fmt --all --check`, `cargo lint`, etc.) as the gate for the compatibility invariants in Features 5, 9.
2. THE spec's `README.md` / tasks doc SHALL document that remote-trigger wiring (e.g. GitHub Actions) is deferred to the `pipeline-foundation` spec (backlog P16) and that `pipeline-foundation` will invoke the same `run_ci_checks` function, so no behavioural divergence between local and remote verdicts is introduced.

### Requirement 10.4: Forward compatibility with `pipeline-foundation`

**User Story:** As a Tokeira maintainer, I want the local CI pipeline shaped so `pipeline-foundation` can re-use it without rewriting, so that we don't ship throwaway code.

#### Acceptance Criteria

1. THE `run_ci_checks` function SHALL take a Dagger client abstraction (the same `DaggerClient` trait as the existing image build/publish/mirror pipelines), so a `pipeline-foundation`-owned remote runner can pass a differently-configured client without changing the check implementations.
2. THE `CiCheckReport` SHALL be `Serialize + Deserialize` via serde so `pipeline-foundation` can ship the report to an external system (artifact store, badge server) without re-serialising the check logic.
3. Adding a new check SHALL be a one-function addition to `ci.rs` plus one variant on the `CiCheck` enum; it SHALL NOT require changes outside `crates/tokeira-build/` or `apps/tkr/src/commands/ci/`.
---

## Feature 11: `tkr compat bump` — driven server-compat bumps

This feature operationalises the Req 5.5 bump protocol as a `tkr` CLI subcommand that walks the maintainer through a Server Compat Bump end-to-end: preflight, evidence gathering from the GitHub API, local commit with the required trailer, local `tkr ci check` validation, push, and PR opening.

### Requirement 11.1: Command surface

**User Story:** As a Tokeira maintainer, I want a single CLI command that drives the entire Server Compat Bump process, so that bumps remain protocol-compliant without me remembering every step.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL expose a `compat bump` subcommand under the existing `tkr compat` group with the signature:

   ```
   tkr compat bump --to <version>
                   [--trigger 1|2|3]
                   [--dry-run]
                   [--derive-surfaces]
                   [--no-open]
                   [--resume | --reset]
                   [--yes]
                   [--json]
   ```

2. `<version>` SHALL be a strict semver string (e.g. `1.29.0`, no `v` prefix to match the `TEMPORAL_SERVER_COMPAT` constant shape).
3. `--trigger` SHALL be required in non-interactive mode (`--yes` or `--json`) and SHALL be interactively prompted otherwise. It SHALL accept only `1`, `2`, or `3`.
4. `--dry-run` SHALL print the proposed PR body, branch name, commit message, and pinned.rs diff to stdout without performing any git mutations, network writes, or branch creation.
5. `--derive-surfaces` SHALL enable the optional upstream proto-diff step described in Feature 11.5. When absent, the generated disposition table SHALL contain a single placeholder row the operator fills manually before pushing.
6. `--no-open` SHALL stop after the local commit + push phases, producing a pushed branch but no GitHub PR. Useful for maintainers who prefer to open the PR via the GitHub web UI.
7. `--resume` and `--reset` SHALL be mutually exclusive; both govern retry behaviour when a bump branch already exists locally (see Req 11.6).
8. Exit status: 0 on success (PR opened or `--no-open` reached its natural end), 1 on any enumerated failure mode from §Failure Modes in design.md, 2 on usage error (enforced by clap).

### Requirement 11.2: Phased execution

**User Story:** As a Tokeira maintainer, I want the bump command's phases to be clearly separated between read-only, local-write, and network-write actions, so that interruptions leave the repository in a knowable state.

#### Acceptance Criteria

1. THE command SHALL execute the following phases in order and SHALL NOT advance past a phase boundary unless the prior phase succeeded:
   - **Phase A — Preflight** (read-only): load `pinned.rs`, validate target version newer via semver, validate working tree clean, validate branch is `main` (or the configured default), validate GitHub credentials via a `GET /user` call.
   - **Phase B — Evidence** (network reads, no writes): enumerate upstream Temporal releases between old and new via the GitHub REST API, fetch each release body, diff the feature matrix against the commit of the previous `Server-Compat-Bump:` trailer, optionally diff the `temporalio/api` proto tree when `--derive-surfaces` is active.
   - **Phase C — Local Mutations** (git write, no network): create the bump branch, update `pinned.rs`, commit with the `Server-Compat-Bump:` trailer, run `tkr ci check` on the branch.
   - **Phase D — Publish** (network writes): push the branch, open the GitHub PR, amend the commit to replace the PR-number placeholder in `pinned.rs` with the real number, force-with-lease re-push.
2. Phase A failures SHALL produce no git or network side effects.
3. Phase B failures SHALL produce no git side effects and SHALL leave no orphaned remote resources.
4. Phase C failures SHALL leave the bump branch in place for debugging but SHALL NOT push it; the operator recovers via `--resume` or `git branch -D`.
5. Phase D failures SHALL leave the pushed branch in place but SHALL NOT leave an orphaned open PR. If PR opening fails mid-step, the tool SHALL write the rendered PR body to a local file and name it in the error output so the operator can open the PR manually.
6. Between Phase B and Phase C, IF not in `--yes` or `--json` mode, THE command SHALL present a confirmation prompt summarising the scope of the bump (count of upstream releases in range, count of upstream-introduced surfaces, count of matrix rows moved).

### Requirement 11.3: Trailer and templated content

**User Story:** As a Tokeira reviewer, I want every bump commit and PR body to follow the same machine-verifiable shape, so that review is fast and mechanical checks work.

#### Acceptance Criteria

1. THE commit created in Phase C SHALL carry a trailer line matching the regex in Req 5.5.5 exactly. THE trailer SHALL be appended using `git interpret-trailers` (not string concatenation) to handle existing trailers safely.
2. THE PR title SHALL follow the pattern `Server compat bump: <old> -> <new> (trigger: <N>)`.
3. THE PR body SHALL be rendered from a single Markdown template stored at `crates/tokeira-build/src/compat_bump/pr_template.md` (or equivalent workspace-relative path). Every placeholder SHALL be filled; rendering with any placeholder left unbound SHALL be an error.
4. THE template SHALL carry, at minimum, the sections enumerated in Req 5.5.4: trigger justification, Upstream Releases table, Disposition Table, Matrix Delta, SDK Test-Suite Evidence, pinned.rs diff, and the reviewer checklist.
5. THE rationale comment in `pinned.rs` SHALL be updated in Phase C with the new claim and a placeholder `PR #?` token; THE placeholder SHALL be replaced with the real PR number in Phase D.

### Requirement 11.4: GitHub API integration

**User Story:** As a Tokeira maintainer, I want the bump tool to handle GitHub authentication, pagination, and rate-limiting predictably, so that transient failures are clearly recoverable.

#### Acceptance Criteria

1. THE command SHALL read GitHub authentication in the following precedence: (a) `GH_TOKEN` environment variable (matches the `gh` CLI convention); (b) `~/.config/tokeira/github-token` file with mode 0600; (c) fail with a diagnostic naming both options and their installation steps.
2. THE authentication token SHALL be validated in Phase A with a `GET /user` call. THE command SHALL verify the token carries at minimum `public_repo` and `pull_requests: write` scopes; missing scopes SHALL fail with an explicit scope list in the error.
3. THE command SHALL use the `octocrab` crate for GitHub REST and GraphQL access. Direct `reqwest` calls to `https://api.github.com` are prohibited to keep one auth path and one rate-limit observer.
4. GITHUB rate-limit responses (HTTP 429 or 403 with `X-RateLimit-Remaining: 0`) SHALL be surfaced with the reset time from `X-RateLimit-Reset` and a suggestion to retry with `--resume` when the window reopens.
5. RELEASE enumeration SHALL paginate automatically via `octocrab::stream`; THE command SHALL cache release bodies locally under `target/tkr/compat-cache/<tag>` so `--resume` does not re-fetch.
6. PR CREATION SHALL use `octocrab::issues()` / `octocrab::pulls()` typed methods. Raw JSON POSTs are prohibited.

### Requirement 11.5: `--derive-surfaces` optional evidence

**User Story:** As a Tokeira maintainer, I want an optional evidence step that produces a skeleton disposition table from the upstream proto diff, so that large bumps don't require manually enumerating every new RPC and field.

#### Acceptance Criteria

1. WHEN `--derive-surfaces` is passed, THE command SHALL resolve the `temporalio/api` proto tag pinned by each Temporal server release in range via `GET /repos/temporalio/temporal/contents/go.mod?ref=<tag>`, parsing the `go.mod` for the `go.temporal.io/api` pin.
2. THE command SHALL shallow-clone `temporalio/api` at the two resolved tags into `target/tkr/compat-cache/api-<tag>/`. Clones SHALL reuse any cached clone and SHALL NOT re-download when the cache is warm.
3. THE command SHALL diff the two proto trees and classify diff entries into: new RPC declarations (`rpc <Name>(`), new message fields (`= <n>;` numbered additions on existing messages), new messages, new enum variants.
4. THE skeleton Disposition Table SHALL list every classified diff entry with the `Tokeira disposition` column blank and the `Matrix row` column auto-filled where a feature-matrix entry names the surface. Where no match exists, the `Matrix row` column SHALL read `—`.
5. `--derive-surfaces` SHALL be best-effort: a clone failure, proto-tree mismatch, or GitHub 404 on the `go.mod` fetch SHALL produce a warning and fall through to the non-derived path, not fail the bump.
6. `--derive-surfaces` is IMPLEMENTED IN TWO STAGES. Stage 1 (landed with the core Feature 11 work) produces the raw diff as a commented appendix in the PR body so the operator can cut-and-paste classifications. Stage 2 (follow-up task) produces the skeleton table structure described in this requirement. Stage 2 is deferred until the core command lands and is exercised in practice.

### Requirement 11.6: Idempotency and retry

**User Story:** As a Tokeira maintainer, I want `tkr compat bump` to be rerunnable after transient failures without leaving half-complete state, so that network flakes and local mistakes are recoverable without a destructive cleanup.

#### Acceptance Criteria

1. RERUNNING `tkr compat bump --to <version>` when a local branch named `compat/server-compat-bump-<old>-<new>` already exists SHALL fail unless `--resume` or `--reset` is passed.
2. `--resume` SHALL replay Phases C and D from the current branch tip, reusing the existing commit if its trailer matches the expected shape, and otherwise amending the commit to carry the correct trailer.
3. `--reset` SHALL delete the local branch, remove any cached evidence from `target/tkr/compat-cache/`, and restart from Phase A.
4. WHEN an open GitHub PR already exists for the same bump branch, `tkr compat bump` SHALL print the PR URL and exit 0 (no duplicate PR opened). `--reset` overrides this behaviour by closing the existing PR and opening a new one.
5. THE command SHALL NOT create temporary commits that are later rewritten by a squash. Phase C creates exactly one commit; Phase D amends that same commit to replace the placeholder PR number.

### Requirement 11.7: Acceptance, testing, and failure-mode coverage

**User Story:** As a Tokeira maintainer, I want `tkr compat bump` to ship with tests that cover every failure mode and the happy path, so that a regression in the command does not break the bump protocol silently.

#### Acceptance Criteria

1. A property test SHALL assert: for any valid `BumpContext` (old version, new version, release list, matrix delta, trigger), the rendered PR body contains every placeholder filled and contains the `Server-Compat-Bump:` trailer in the commit message section.
2. A property test SHALL assert: the generated commit message is exactly byte-equal across repeated renderings with the same inputs (deterministic).
3. A unit test SHALL assert: the `Server-Compat-Bump:` trailer regex in Req 5.5.5 accepts all well-formed trailers and rejects common malformations (missing trigger, non-semver versions, wrong arrow, wrong trigger digit).
4. Integration tests against a mocked `octocrab` client SHALL exercise Phase A + Phase B + Phase D (the Phase C local-git mutations are exercised against a `tempfile::TempDir` fixture holding a fresh git repo).
5. ONE integration test, gated behind the `integration-test` feature flag and the `--ignored` attribute, SHALL open a real PR against a fork of the tokeira repository and then close it. Running this test requires a test-scope `GH_TOKEN`; it is not part of the default `cargo test` set per AGENTS.md.
6. EVERY failure mode enumerated in the design's §Failure Modes table SHALL have either a unit test (for pure-logic failures) or an integration test (for IO failures via mocked clients).

### Requirement 11.8: Cross-references and forward compatibility

**User Story:** As a Tokeira maintainer, I want `tkr compat bump` to integrate cleanly with the existing `tkr ci check` pipeline and with the forthcoming `pipeline-foundation` remote triggers, so that the bump process does not diverge between local and remote environments.

#### Acceptance Criteria

1. `tkr compat bump` Phase C SHALL invoke `tkr ci check` (the Feature 10 Dagger pipeline) on the bump branch tip before attempting Phase D. IF `tkr ci check` fails, THE bump SHALL abort and the operator SHALL be pointed at the failing check output; Phase D never runs on a red branch.
2. THE `Server-Compat-Bump:` trailer check SHALL be added to `tkr ci check` as part of the proto-monotonicity / version-pin family (renamed from `run_proto_monotonicity` to `run_version_pin_checks` in the implementation per design). THE check SHALL fire only when `pinned.rs` changed in the tip commit.
3. `pipeline-foundation` (backlog P16) SHALL re-use the `tkr ci check` version-pin family when wiring remote triggers for bump PRs. No separate CI logic lives in a GHA workflow.
4. THE `CODEOWNERS` file created per Req 5.5.6 SHALL be honoured by `pipeline-foundation`'s eventual branch-protection rules without re-writing, by naming the `pinned.rs` path and the owner team with a portable syntax GH recognises directly.
