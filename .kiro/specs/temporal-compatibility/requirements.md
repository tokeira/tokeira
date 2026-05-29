# Requirements Document

## Introduction

Tokeira is a Temporal-compatible durable execution engine implemented in Rust. Its compatibility promise must be explicit, testable, queryable, and bound to release artefacts.

Tokeira must preserve Temporal SDK compatibility without forking or extending Temporal’s upstream protobuf definitions. Standard Temporal SDKs must see a normal Temporal server surface. Tokeira-specific compatibility metadata, build provenance, feature state, and capability details must be exposed through Tokeira-owned protobuf services.

Tokeira has adopted Buffa and connect-rust for Tokeira-native gRPC communication, including controller and autoscaler control-plane APIs. This specification therefore requires any Tokeira-owned protobuf/RPC surface that exposes build, compatibility, feature, or capability metadata to use the Buffa + connect-rust stack.

Dagger is the authoritative build and CI substrate for Tokeira. Local developer checks, remote CI checks, generated-code validation, versioned builds, source-tree hashing, compatibility checks, and release artefact validation SHALL execute through the same Dagger functions. Build metadata SHALL be derived inside the Dagger execution graph from repository state and checked-in configuration. Build metadata SHALL NOT be supplied by ambient environment variables.

Temporal upstream RPCs remain governed by the vendored Temporal API compatibility contract. Tokeira-owned RPCs use Tokeira-owned proto packages and Buffa/connect-rust generated code.

---

## Goals

This specification SHALL define:

1. build-time metadata and release provenance,
2. Dagger as the authoritative local and remote build/CI substrate,
3. programmatic derivation of build metadata from repository state,
4. Temporal proto pinning and server compatibility claims,
5. a canonical feature matrix,
6. a broader compatibility-surface model beyond RPC names,
7. a conservative SDK compatibility matrix,
8. a strict upstream-compatible implementation of Temporal `GetSystemInfo`,
9. a Tokeira-native compatibility service generated with Buffa and served with connect-rust,
10. local and remote compatibility CLI commands,
11. Dagger-based CI guardrails using Dagger lockfile support,
12. manual compatibility bump governance.

---

## Non-goals

This specification SHALL NOT define:

1. the implementation of every individual Temporal feature,
2. storage schema compatibility,
3. release automation,
4. GitHub PR creation,
5. automatic release-note scraping,
6. full Temporal SDK conformance orchestration,
7. Buildkite or other remote CI wiring,
8. dashboards or long-term observability views.

The previously proposed `tkr compat bump` automation is deferred. This specification may define metadata and checks that future automation consumes, but the MVP SHALL remain manually reviewable.

---

## Design Principles

1. **Do not fork Temporal’s SDK-facing proto surface.**
2. **Expose Tokeira metadata through Tokeira-owned services.**
3. **Use Buffa + connect-rust for Tokeira-owned build and capability RPCs.**
4. **Treat `TEMPORAL_SERVER_COMPAT` as an evidence-backed claim.**
5. **Treat proto compatibility and behavioural compatibility as separate claims.**
6. **Prefer conservative feature states until conformance evidence exists.**
7. **Make compatibility metadata deterministic and digestible.**
8. **Use Dagger as the build and CI authority.**
9. **Derive build metadata; do not inject it.**
10. **Use Dagger lockfiles to reduce CI supply-chain drift.**

---

## Glossary

### `TOKEIRA_VERSION`

The semantic version of the Tokeira binary, derived from checked-in Cargo package or workspace metadata.

### `TOKEIRA_GIT_SHA`

The git commit identifier derived from the checked-out repository.

Dirty builds SHALL include a dirty marker.

### `TEMPORAL_PROTO_VERSION`

The upstream `temporalio/api` version vendored by Tokeira.

This value SHALL refer only to the unmodified upstream Temporal protobuf mirror.

### `TEMPORAL_SERVER_COMPAT`

The highest Temporal server version for which Tokeira claims SDK-relevant behavioural compatibility across the supported SDK matrix.

This SHALL be an evidence-backed compatibility claim, not a direct derivation from `TEMPORAL_PROTO_VERSION`.

### `SOURCE_TREE_HASH`

A deterministic SHA-256 digest of the source tree after applying the configured workspace exclusions.

### `BuildInfo`

A structured metadata value containing Tokeira version, git SHA, source-tree hash, Rust toolchain, Temporal proto version, Temporal server compatibility claim, and matrix digests.

### `Build Metadata Manifest`

A deterministic generated file produced by the Dagger build graph. It contains the derived metadata that Cargo embeds into binaries.

The manifest is generated from repository state and checked-in configuration. It is not supplied through environment variables.

### `FeatureState`

The support state for a known Temporal feature.

Allowed values:

- `Implemented`
- `Experimental`
- `Stubbed`
- `Unsupported`

### `CompatibilitySurface`

A classified unit of Temporal compatibility.

Examples include RPCs, request fields, response fields, history events, command attributes, enum variants, capability flags, error details, and behavioural invariants.

### `FeatureMatrix`

The canonical list of known Temporal features and their Tokeira support state.

### `SdkMatrix`

The canonical list of supported Temporal SDK languages, version ranges, known incompatible versions, and conformance evidence.

### Standard Temporal handshake

The upstream-compatible `GetSystemInfo` RPC implemented by Tokeira through Temporal’s standard `WorkflowService`.

### Tokeira compatibility service

A Tokeira-owned RPC service exposing build metadata, feature state, SDK compatibility, capability metadata, and conformance evidence.

### Buffa

The protobuf implementation used for Tokeira-owned protobuf messages in this specification.

### connect-rust

The RPC implementation used for Tokeira-owned compatibility, build, and capability services in this specification.

### Dagger lockfile

The committed `.dagger/lock` file that pins Dagger-resolved mutable inputs such as container image references, Git references, and HTTP fetches.

### Build mode: `dev`

A Dagger build mode for local development. Allows dirty repository state. Derives build metadata where available but does not require a clean git commit or full provenance.

### Build mode: `versioned`

A Dagger build mode for CI and release artefacts. Requires a clean git commit, rejects dirty repository state, derives full build metadata from repository state and checked-in configuration, and verifies embedded `BuildInfo` after build.

---

## Requirements

---

## Feature 1: Build-Time Compatibility Metadata

### Requirement 1: Build metadata crate

**User Story:** As a Tokeira developer, I want build metadata to be owned by one crate, so that runtime services, CLI commands, and tests cannot drift.

#### Acceptance Criteria

1. THE workspace SHALL include a crate named `tokeira-build-info`.
2. THE crate SHALL expose `TOKEIRA_VERSION`.
3. THE crate SHALL expose `TOKEIRA_GIT_SHA`.
4. THE crate SHALL expose `TEMPORAL_PROTO_VERSION`.
5. THE crate SHALL expose `TEMPORAL_SERVER_COMPAT`.
6. THE crate SHALL expose `RUST_TOOLCHAIN`.
7. THE crate SHALL expose `SOURCE_TREE_HASH`.
8. THE crate SHALL expose `FEATURE_MATRIX_DIGEST`.
9. THE crate SHALL expose `SDK_MATRIX_DIGEST`.
10. THE crate SHALL expose `fn summary() -> BuildInfo`.
11. THE crate SHALL have no runtime dependencies beyond `std`.
12. THE crate SHALL NOT own JSON, YAML, table, protobuf, or terminal rendering.
13. THE crate SHALL NOT treat environment variables as authoritative build metadata.
14. THE crate SHALL embed metadata derived from repository state and checked-in configuration.

### Requirement 2: Programmatic metadata derivation

**User Story:** As a release engineer, I want build metadata to be derived from the repository and checked-in configuration, so that release provenance does not depend on externally supplied environment variables.

#### Acceptance Criteria

1. WHEN the Dagger build metadata derivation function runs, THE function SHALL derive `TOKEIRA_VERSION` from checked-in Cargo package or workspace metadata.
2. WHEN the Dagger build metadata derivation function runs, THE function SHALL derive `TOKEIRA_GIT_SHA` from the checked-out git repository.
3. WHEN the Dagger build metadata derivation function runs, THE function SHALL detect whether the checked-out git repository is dirty.
4. WHEN the checked-out git repository is dirty, THE derived `TOKEIRA_GIT_SHA` SHALL include a dirty marker.
5. WHEN the Dagger build metadata derivation function runs, THE function SHALL derive `SOURCE_TREE_HASH` by hashing the checked-out source tree using the configured deterministic exclusion list.
6. WHEN the Dagger build metadata derivation function runs, THE function SHALL derive `RUST_TOOLCHAIN` from `rust-toolchain.toml`.
7. WHEN the Dagger build metadata derivation function runs, THE function SHALL derive `TEMPORAL_PROTO_VERSION` from checked-in compatibility configuration.
8. WHEN the Dagger build metadata derivation function runs, THE function SHALL derive `TEMPORAL_SERVER_COMPAT` from checked-in compatibility configuration.
9. WHEN the Dagger build metadata derivation function runs, THE function SHALL derive feature and SDK matrix digests from checked-in matrix definitions.
10. THE build metadata derivation SHALL NOT require user-supplied environment variables.
11. THE build metadata derivation SHALL NOT require CI-supplied environment variables.
12. THE build metadata derivation SHALL fail with a clear error if required repository metadata cannot be derived.
13. THE build metadata derivation SHALL NOT embed wall-clock timestamps.
14. THE build metadata derivation SHALL be reproducible for the same repository state and checked-in configuration.

### Requirement 3: Generated build metadata manifest

**User Story:** As a maintainer, I want Cargo builds to consume a deterministic generated metadata manifest, so that build metadata is controlled by Dagger and not by ambient environment state.

#### Acceptance Criteria

1. WHEN the Dagger build function prepares a Cargo build, THE function SHALL generate a deterministic build metadata manifest.
2. THE build metadata manifest SHALL contain all fields required by `tokeira-build-info`.
3. THE build metadata manifest SHALL be generated from repository state and checked-in configuration.
4. THE build metadata manifest SHALL NOT be generated from ambient CI environment variables.
5. THE build metadata manifest SHALL NOT contain wall-clock timestamps.
6. THE build metadata manifest SHALL be stable for the same repository state and checked-in configuration.
7. WHEN Cargo builds a Tokeira binary through Dagger, THE `tokeira-build-info` build process SHALL embed metadata from the generated manifest.
8. WHEN the generated manifest is missing during a versioned build, THE build SHALL fail with a clear error.
9. WHEN the generated manifest is malformed, THE build SHALL fail with a clear error.
10. WHEN the generated manifest is inconsistent with checked-in compatibility configuration, THE build SHALL fail with a clear error.
11. THE manifest format SHALL be documented.
12. THE manifest format SHALL be covered by tests.

### Requirement 4: CI release provenance validation

**User Story:** As an operator, I want release binaries to prove their provenance was derived from the repository, so that deployments are auditable and not dependent on injected CI metadata.

#### Acceptance Criteria

1. WHEN compatibility checks run in Dagger, THE checks SHALL verify that `TOKEIRA_GIT_SHA` was derived from the checked-out git repository.
2. WHEN compatibility checks run in Dagger, THE checks SHALL verify that `SOURCE_TREE_HASH` was derived from the checked-out source tree.
3. WHEN versioned checks run in Dagger, THE checks SHALL fail if the git repository is dirty.
4. WHEN versioned checks run in Dagger, THE checks SHALL fail if the git commit cannot be derived.
5. WHEN versioned checks run in Dagger, THE checks SHALL fail if `SOURCE_TREE_HASH` cannot be recomputed independently.
6. WHEN the independently recomputed source-tree hash differs from the embedded `SOURCE_TREE_HASH`, THE checks SHALL fail.
7. THE Dagger checks SHALL NOT provide build metadata through environment variables.
8. THE Dagger checks SHALL validate derived metadata after build rather than supplying metadata before build.
9. THE Dagger versioned path SHALL reject incomplete repository provenance.
10. THE Dagger versioned path SHALL reject non-deterministic metadata.

### Requirement 5: Source-tree hash

**User Story:** As a maintainer, I want a deterministic source-tree hash, so that two builds from the same source can be compared across machines.

#### Acceptance Criteria

1. THE source-tree hash SHALL be a SHA-256 digest.
2. WHEN computing `SOURCE_TREE_HASH`, THE hash input SHALL use deterministic file ordering.
3. WHEN computing `SOURCE_TREE_HASH`, THE hash input SHALL include relative file paths.
4. WHEN computing `SOURCE_TREE_HASH`, THE hash input SHALL include file contents.
5. WHEN computing `SOURCE_TREE_HASH`, THE hash input SHALL exclude build artefacts.
6. WHEN computing `SOURCE_TREE_HASH`, THE hash input SHALL exclude editor metadata.
7. WHEN computing `SOURCE_TREE_HASH`, THE hash input SHALL exclude OS junk files.
8. WHEN computing `SOURCE_TREE_HASH`, THE hash input SHALL exclude local environment files.
9. WHEN computing `SOURCE_TREE_HASH`, THE hash input SHALL exclude Dagger runtime caches.
10. THE exclusion list SHALL be declared in one checked-in location.
11. THE Dagger pipeline and local validation helpers SHALL use the same exclusion list.
12. THE source-tree hash SHALL be derived inside the Dagger build graph for versioned builds.

### Requirement 6: Version output

**User Story:** As an operator, I want `tokeirad --version` to expose compatibility metadata, so that I can verify what is running.

#### Acceptance Criteria

1. WHEN `tokeirad --version` is executed, THE binary SHALL print `TOKEIRA_VERSION`.
2. WHEN `tokeirad --version --verbose` is executed, THE binary SHALL print every `BuildInfo` field.
3. WHEN `tokeirad --version --json` is executed, THE binary SHALL print a stable JSON representation of `BuildInfo`.
4. THE JSON rendering SHALL be implemented outside `tokeira-build-info`.
5. THE JSON representation SHALL use stable field names.
6. THE JSON representation SHALL be covered by a golden-file or snapshot test.
7. THE version output SHALL identify the build mode (`dev` or `versioned`).
8. THE version output SHALL NOT include wall-clock build timestamps.

### Requirement 7: Startup provenance log

**User Story:** As an operator, I want each Tokeira process to log build provenance on startup, so that logs identify the deployed version.

#### Acceptance Criteria

1. WHEN `tokeirad` starts, THE process SHALL emit exactly one structured startup log event containing `BuildInfo` covering the host process and its embedded edge and projection components.
2. WHEN `tokeira-controller` starts, THE process SHALL emit exactly one structured startup log event containing `BuildInfo`.
3. WHEN `tokeira-autoscaler` starts, THE process SHALL emit exactly one structured startup log event containing `BuildInfo`.
4. THE startup log SHALL NOT truncate hashes, digests, or version strings.
5. THE startup log SHALL NOT include wall-clock build timestamps.
6. THE startup log SHALL include enough metadata to identify source version, source-tree hash, Temporal proto version, and Temporal server compatibility claim.

---

## Feature 2: Buffa + connect-rust for Tokeira-Owned Compatibility RPC

### Requirement 8: Tokeira-owned proto generation

**User Story:** As a Tokeira developer, I want Tokeira-owned compatibility protos generated with Buffa, so that the control-plane proto stack is consistent with controller and autoscaler communication.

#### Acceptance Criteria

1. THE Tokeira compatibility proto package SHALL be Tokeira-owned.
2. THE Tokeira compatibility proto files SHALL NOT live inside the vendored upstream Temporal proto tree.
3. THE Tokeira compatibility proto files SHALL be generated with Buffa.
4. THE generated message types for Tokeira compatibility metadata SHALL use Buffa-generated Rust types.
5. THE generated Tokeira compatibility code SHALL be checked for freshness in Dagger CI.
6. THE Tokeira compatibility proto generation SHALL be reproducible from checked-in proto files and checked-in generation configuration.
7. THE Tokeira compatibility proto package SHALL use a stable versioned package name.
8. THE initial package name SHOULD be `tokeira.compatibility.v1`.

### Requirement 9: connect-rust service implementation

**User Story:** As a Tokeira operator, I want build and capability metadata exposed through the same RPC stack as Tokeira’s control plane, so that internal tooling is coherent.

#### Acceptance Criteria

1. THE Tokeira compatibility service SHALL be generated with connect-rust.
2. THE Tokeira compatibility service SHALL be served with connect-rust.
3. THE Tokeira compatibility client used by `tkr` SHALL use connect-rust.
4. THE Tokeira compatibility service SHALL support protobuf binary requests and responses.
5. THE Tokeira compatibility service MAY support JSON protobuf requests and responses where connect-rust provides this safely.
6. THE Tokeira compatibility service SHALL NOT use tonic-generated service code.
7. THE Tokeira compatibility service SHALL NOT use prost-generated Tokeira-owned message types.
8. THE use of tonic or prost for upstream Temporal SDK-facing services SHALL be treated as separate from this requirement.

### Requirement 10: Tokeira build and capability surfaces

**User Story:** As an architecture maintainer, I want every Tokeira-owned RPC that surfaces build or capability details to use Buffa and connect-rust, so that the metadata surface is not fragmented.

#### Acceptance Criteria

1. WHERE a Tokeira-owned gRPC or ConnectRPC service exposes `BuildInfo`, THE service SHALL use Buffa-generated messages.
2. WHERE a Tokeira-owned gRPC or ConnectRPC service exposes feature state, THE service SHALL use Buffa-generated messages.
3. WHERE a Tokeira-owned gRPC or ConnectRPC service exposes capability state, THE service SHALL use Buffa-generated messages.
4. WHERE a Tokeira-owned gRPC or ConnectRPC service exposes SDK compatibility metadata, THE service SHALL use Buffa-generated messages.
5. WHERE a Tokeira-owned gRPC or ConnectRPC service exposes compatibility evidence, THE service SHALL use Buffa-generated messages.
6. WHERE a Tokeira-owned service exposes this metadata over RPC, THE service SHALL use connect-rust handlers.
7. THE controller and autoscaler SHALL follow the same Buffa + connect-rust convention for any build or capability metadata they expose.
8. THE runtime and edge services SHALL follow the same Buffa + connect-rust convention for any Tokeira-owned build or capability metadata they expose.

### Requirement 11: Separation from Temporal upstream API

**User Story:** As a Temporal SDK user, I want standard Temporal APIs to remain unmodified, so that Tokeira remains compatible with existing SDKs and tools.

#### Acceptance Criteria

1. THE vendored Temporal proto files SHALL NOT import Tokeira-owned proto files.
2. THE vendored Temporal proto files SHALL NOT reference Buffa-specific options.
3. THE vendored Temporal proto files SHALL NOT reference connect-rust-specific options.
4. THE standard Temporal `WorkflowService` SHALL NOT expose Tokeira-owned metadata fields.
5. THE standard Temporal `GetSystemInfoResponse` SHALL NOT include Tokeira-specific fields.
6. THE Tokeira compatibility service SHALL be the RPC surface for Tokeira-specific build and capability metadata.
7. THE standard Temporal SDK handshake SHALL remain independent of the Tokeira compatibility service.

### Requirement 12: Generated-code supply-chain control

**User Story:** As a maintainer, I want Buffa and connect-rust code generation to be pinned and reviewed, so that generated compatibility code cannot drift silently.

#### Acceptance Criteria

1. THE versions of Buffa code generation tools SHALL be pinned.
2. THE versions of connect-rust code generation tools SHALL be pinned.
3. THE pinned versions SHALL be visible in checked-in configuration or lockfiles.
4. WHEN Buffa or connect-rust codegen versions change, THE generated output SHALL be refreshed.
5. WHEN generated output changes, THE pull request SHALL include the generated-code diff.
6. WHEN generated output changes unexpectedly, THE Dagger freshness check SHALL fail.
7. THE Dagger lockfile policy SHALL apply to Buffa and connect-rust codegen acquisition when Dagger resolves those inputs.

---

## Feature 3: Temporal Feature Matrix

### Requirement 13: Feature state taxonomy

**User Story:** As a maintainer, I want every known Temporal feature to have an explicit support state, so that unsupported behaviour is visible and intentional.

#### Acceptance Criteria

1. THE system SHALL define `FeatureState::Implemented`.
2. THE system SHALL define `FeatureState::Experimental`.
3. THE system SHALL define `FeatureState::Stubbed`.
4. THE system SHALL define `FeatureState::Unsupported`.
5. WHERE a feature is `Implemented`, THE feature SHALL be expected to behave compatibly with `TEMPORAL_SERVER_COMPAT`.
6. WHERE a feature is `Experimental`, THE feature SHALL be implemented behind runtime configuration.
7. WHERE a feature is `Experimental`, THE feature SHALL NOT be advertised as generally available by default.
8. WHERE a feature is `Stubbed`, THE wire surface MAY exist but THE implementation SHALL fail predictably with a Temporal-compatible error.
9. WHERE a feature is `Unsupported`, THE system SHALL NOT advertise support and SHALL reject use predictably.

### Requirement 14: Compatibility surface model

**User Story:** As a compatibility reviewer, I want compatibility coverage to include more than RPC names, so that subtle SDK assumptions are not missed.

#### Acceptance Criteria

1. THE system SHALL define `CompatibilitySurfaceKind::Rpc`.
2. THE system SHALL define `CompatibilitySurfaceKind::RequestField`.
3. THE system SHALL define `CompatibilitySurfaceKind::ResponseField`.
4. THE system SHALL define `CompatibilitySurfaceKind::HistoryEvent`.
5. THE system SHALL define `CompatibilitySurfaceKind::CommandAttribute`.
6. THE system SHALL define `CompatibilitySurfaceKind::EnumVariant`.
7. THE system SHALL define `CompatibilitySurfaceKind::CapabilityFlag`.
8. THE system SHALL define `CompatibilitySurfaceKind::ErrorDetail`.
9. THE system SHALL define `CompatibilitySurfaceKind::BehaviouralInvariant`.
10. EVERY compatibility surface SHALL have a stable identifier.
11. EVERY compatibility surface SHALL map to exactly one feature entry unless it is explicitly cross-cutting.
12. EVERY cross-cutting surface SHALL explain why it cannot be owned by a single feature.

### Requirement 15: Feature matrix source of truth

**User Story:** As a developer, I want the feature matrix to be the single source of truth, so that compile-time gates, runtime dispatch, and CLI reporting agree.

#### Acceptance Criteria

1. THE workspace SHALL include a `tokeira-compatibility` crate.
2. THE `tokeira-compatibility` crate SHALL own the canonical `FEATURE_MATRIX`.
3. EACH feature entry SHALL include `id`.
4. EACH feature entry SHALL include `name`.
5. EACH feature entry SHALL include `state`.
6. EACH feature entry SHALL include `surfaces`.
7. EACH feature entry SHALL include `capability_field` when applicable.
8. EACH feature entry SHALL include `dynamic_config_key` when applicable.
9. EACH feature entry SHALL include `notes`.
10. EACH feature entry SHALL include `evidence`.
11. EACH feature ID SHALL be a continuous name in kebab-case whose behaviour is consistent across versions.
12. RENAMING a feature ID SHALL be treated as a breaking change.
13. EACH feature ID SHALL be unique.
14. THE matrix SHALL be sorted by feature ID.
15. THE test suite SHALL fail if the matrix is not sorted.
16. THE feature matrix digest SHALL be computed from the declared order.

### Requirement 16: Conservative initial states

**User Story:** As a maintainer, I want complex Temporal features to start conservatively, so that Tokeira does not overclaim compatibility.

#### Acceptance Criteria

1. WHEN seeding the initial matrix, THE system SHALL mark a feature `Implemented` only when targeted tests verify SDK-visible behaviour.
2. WHEN a feature has a handler but lacks behavioural conformance evidence, THE system SHALL mark it `Experimental` or `Stubbed`.
3. WHERE workflow queries are present, THE feature SHALL NOT be marked `Implemented` until query ordering against prior committed signals is tested.
4. WHERE sticky task queues are present, THE feature SHALL NOT be marked `Implemented` until sticky replay, cache miss, and fallback behaviour are tested.
5. WHERE eager workflow start is present, THE feature SHALL NOT be marked `Implemented` until SDK eager-start semantics are tested.
6. WHERE worker versioning is present, THE feature SHALL NOT be marked `Implemented` until SDK compatibility tests cover the relevant SDK versions.
7. WHERE Nexus is present, THE feature SHALL include explicit evidence before being marked `Implemented`.
8. WHERE workflow updates are present, THE feature SHALL include explicit evidence before being marked `Implemented`.
9. WHERE reset is present, THE feature SHALL include explicit evidence before being marked `Implemented`.
10. WHERE child workflows are present, THE feature SHALL include explicit evidence before being marked `Implemented`.
11. WHERE cron or schedules are present, THE feature SHALL include explicit evidence before being marked `Implemented`.
12. WHERE continue-as-new is present, THE feature SHALL include explicit evidence before being marked `Implemented`.
13. WHERE search attributes are present, THE feature SHALL include explicit evidence before being marked `Implemented`.

### Requirement 17: RPC completeness property

**User Story:** As a compatibility reviewer, I want every upstream RPC classified, so that no Temporal endpoint is accidentally ignored.

#### Acceptance Criteria

1. WHEN tests run, THE system SHALL enumerate every RPC in the vendored upstream `WorkflowService`.
2. WHEN tests run, THE system SHALL enumerate every RPC in the vendored upstream `OperatorService`.
3. FOR EACH enumerated RPC, THE test suite SHALL assert that exactly one feature entry owns it.
4. FOR EACH RPC referenced by a feature entry, THE test suite SHALL assert that the RPC exists in the vendored upstream proto.
5. THE RPC completeness property SHALL NOT be treated as sufficient proof of behavioural compatibility.
6. THE RPC completeness property SHALL be documented as a wire-surface guardrail.

### Requirement 18: Runtime feature dispatch

**User Story:** As an edge developer, I want runtime dispatch to respect feature state, so that unavailable features fail consistently.

#### Acceptance Criteria

1. THE `tokeira-compatibility` crate SHALL expose a runtime dispatch helper.
2. WHEN a request targets an `Implemented` feature, THE system SHALL allow the real handler to execute.
3. WHEN a request targets an `Experimental` feature and runtime configuration enables it, THE system SHALL allow the real handler to execute.
4. WHEN a request targets an `Experimental` feature and runtime configuration disables it, THE system SHALL return a Temporal-compatible unavailable error.
5. WHEN a request targets a `Stubbed` feature, THE system SHALL return a Temporal-compatible unimplemented or failed-precondition error.
6. WHEN a request targets an `Unsupported` feature, THE system SHALL return a Temporal-compatible unimplemented error.
7. THE dispatch helper SHALL emit a metric tagged with feature ID and feature state.

---

## Feature 4: SDK Compatibility Matrix

### Requirement 19: SDK matrix structure

**User Story:** As an operator, I want Tokeira to publish known SDK compatibility, so that I can assess client upgrade risk.

#### Acceptance Criteria

1. THE system SHALL define a canonical `SDK_MATRIX`.
2. EACH SDK entry SHALL include language.
3. EACH SDK entry SHALL include minimum supported version.
4. EACH SDK entry SHALL include maximum tested version.
5. EACH SDK entry SHALL include known incompatible versions.
6. EACH SDK entry SHALL include conformance status.
7. EACH SDK entry SHALL include evidence.
8. EACH SDK entry SHALL include `verification_state`.
9. THE initial SDK languages SHALL include Go, TypeScript, Python, Java, and .NET.
10. THE allowed verification states SHALL include `Untested`, `SmokeTested`, `ConformancePartial`, and `ConformancePassing`.
11. THE matrix SHALL NOT imply full SDK support unless the verification state supports that claim.
12. THE matrix SHALL be exposed by the Tokeira compatibility service.
13. THE matrix SHALL be printed by `tkr compat show`.

### Requirement 20: SDK matrix data model

**User Story:** As a developer, I want static and owned SDK matrix types, so that runtime reporting and serialization tests are straightforward.

#### Acceptance Criteria

1. THE static SDK matrix SHALL use borrow-friendly static string fields where appropriate.
2. THE system SHALL define an owned SDK matrix representation.
3. WHEN the static SDK matrix is serialized to JSON, THE JSON SHALL deserialize into the owned representation.
4. WHEN the owned representation is re-digested, THE digest SHALL match the static SDK matrix digest.
5. WHEN a compatibility-significant SDK field changes, THE SDK matrix digest SHALL change.

### Requirement 21: SDK version ordering

**User Story:** As a maintainer, I want SDK compatibility ranges to be internally consistent, so that the published matrix is credible.

#### Acceptance Criteria

1. WHEN tests run, THE system SHALL parse every SDK minimum version as semantic versioning.
2. WHEN tests run, THE system SHALL parse every SDK maximum tested version as semantic versioning.
3. FOR EACH SDK entry, THE minimum version SHALL be less than or equal to the maximum tested version.
4. FOR EACH SDK entry, THE maximum tested version SHALL NOT be listed as known incompatible.
5. FOR EACH known incompatible version, THE matrix SHALL include a reason.

### Requirement 22: Server compatibility claim

**User Story:** As a maintainer, I want `TEMPORAL_SERVER_COMPAT` to mean behavioural compatibility, so that Tokeira does not overclaim based on release dates.

#### Acceptance Criteria

1. `TEMPORAL_SERVER_COMPAT` SHALL identify the highest Temporal server version for which Tokeira has no known SDK-breaking behavioural divergence across the supported SDK matrix.
2. A newer Temporal release SHALL NOT be sufficient reason to bump `TEMPORAL_SERVER_COMPAT`.
3. A protobuf bump SHALL NOT be sufficient reason to bump `TEMPORAL_SERVER_COMPAT`.
4. A calendar drift threshold MAY trigger review of `TEMPORAL_SERVER_COMPAT`.
5. THE review SHALL NOT update `TEMPORAL_SERVER_COMPAT` unless conformance evidence supports the new claim.
6. WHERE known divergences exist, THE compatibility metadata SHALL expose them through the feature matrix or SDK matrix.

---

## Feature 5: Standard Temporal `GetSystemInfo`

### Requirement 23: Upstream message shape

**User Story:** As a Temporal SDK user, I want Tokeira’s Temporal handshake to use the upstream Temporal protobuf shape, so that standard SDKs and tools do not see a forked API.

#### Acceptance Criteria

1. THE vendored Temporal `GetSystemInfoRequest` SHALL exactly mirror the upstream Temporal proto.
2. THE vendored Temporal `GetSystemInfoResponse` SHALL exactly mirror the upstream Temporal proto.
3. THE vendored Temporal `Capabilities` message SHALL exactly mirror the upstream Temporal proto.
4. THE system SHALL NOT add Tokeira-specific fields to upstream Temporal protobuf messages.
5. THE system SHALL NOT reserve custom field ranges inside upstream Temporal protobuf messages.
6. THE proto sync check SHALL fail if an upstream Temporal proto file is locally patched.
7. THE standard Temporal `GetSystemInfo` handler SHALL return only fields defined by the upstream Temporal API.
8. THE standard Temporal `GetSystemInfo` handler SHALL NOT use the Tokeira compatibility service response as its protobuf schema.

### Requirement 24: Server version in handshake

**User Story:** As a Temporal SDK, I want `server_version` to be populated consistently, so that SDK-side feature logic has a stable signal.

#### Acceptance Criteria

1. WHEN standard `GetSystemInfo` is called, THE response SHALL set `server_version` to `TEMPORAL_SERVER_COMPAT`.
2. THE value of `server_version` SHALL NOT be derived from `TEMPORAL_PROTO_VERSION`.
3. THE value of `server_version` SHALL NOT be derived from `TOKEIRA_VERSION`.
4. WHERE `TEMPORAL_SERVER_COMPAT` is empty, THE Dagger build SHALL fail before a runtime can be produced.

### Requirement 25: Upstream capability flags

**User Story:** As a Temporal SDK, I want upstream capability flags to reflect implemented SDK-visible behaviour, so that client behaviour remains safe.

#### Acceptance Criteria

1. WHEN standard `GetSystemInfo` is called, THE response SHALL populate upstream capability fields from the feature matrix.
2. WHERE a feature is `Implemented` and maps to an upstream capability field, THE capability flag SHALL be `true`.
3. WHERE a feature is `Stubbed` and maps to an upstream capability field, THE capability flag SHALL be `false`.
4. WHERE a feature is `Unsupported` and maps to an upstream capability field, THE capability flag SHALL be `false`.
5. WHERE a feature is `Experimental` and maps to an upstream capability field, THE default standard handshake SHALL return `false` unless the feature is globally enabled for standard SDK use.
6. THE standard `GetSystemInfo` handler SHALL NOT expose namespace-specific capability differences.
7. THE standard `GetSystemInfo` handler SHALL NOT include Tokeira build metadata.
8. THE standard `GetSystemInfo` handler SHALL NOT include Tokeira feature-state maps.
9. THE standard `GetSystemInfo` handler SHALL NOT include Tokeira SDK matrix data.

### Requirement 26: Namespace-specific behaviour

**User Story:** As an operator, I want namespace-specific feature gates to be enforced at request time, so that the global SDK handshake remains safe.

#### Acceptance Criteria

1. WHERE feature availability is namespace-specific, THE actual RPC handler SHALL enforce the namespace-specific gate.
2. THE standard `GetSystemInfo` response SHALL be treated as global/default capability metadata.
3. WHEN a namespace-specific feature is disabled, THE relevant RPC handler SHALL reject the request with a Temporal-compatible error.
4. WHEN a namespace-specific feature is enabled, THE relevant RPC handler MAY proceed according to feature state and runtime dispatch.
5. THE Tokeira compatibility service MAY expose namespace-specific effective feature state when the caller supplies a namespace.

### Requirement 27: Handshake consistency property

**User Story:** As a maintainer, I want every upstream capability flag intentionally mapped, so that SDK-visible claims are reviewed.

#### Acceptance Criteria

1. WHEN tests run, THE system SHALL enumerate every field in the upstream `Capabilities` message.
2. FOR EACH upstream capability field, THE test suite SHALL assert that exactly one feature entry maps to it or that it is explicitly documented as intentionally unmapped.
3. FOR EACH feature entry with a capability field, THE test suite SHALL assert that the field exists in the upstream `Capabilities` message.
4. WHEN a capability field is added upstream, THE test suite SHALL fail until the feature matrix is updated.
5. WHEN a capability field is removed upstream, THE test suite SHALL fail until the feature matrix is updated.

---

## Feature 6: Tokeira Compatibility Service

### Requirement 28: Service ownership

**User Story:** As a Tokeira operator, I want rich compatibility metadata without modifying Temporal’s standard API, so that operator tooling can be powerful while SDK compatibility remains clean.

#### Acceptance Criteria

1. THE system SHALL define a Tokeira-owned compatibility service.
2. THE service SHALL live outside the upstream Temporal proto namespace.
3. THE service SHALL be generated with Buffa and connect-rust.
4. THE service SHALL expose `GetCompatibility`.
5. THE service MAY expose `ListCompatibilitySurfaces`.
6. THE service MAY expose `GetFeature`.
7. THE service MAY expose `GetSdkCompatibility`.
8. THE service SHALL NOT be required by standard Temporal SDKs.
9. THE service SHALL be consumed by `tkr compat show --remote`.

### Requirement 29: `GetCompatibility` response

**User Story:** As an operator, I want `GetCompatibility` to return the full compatibility picture, so that I can diagnose a live deployment.

#### Acceptance Criteria

1. WHEN `GetCompatibility` is called, THE response SHALL include `BuildInfo`.
2. WHEN `GetCompatibility` is called, THE response SHALL include `TEMPORAL_PROTO_VERSION`.
3. WHEN `GetCompatibility` is called, THE response SHALL include `TEMPORAL_SERVER_COMPAT`.
4. WHEN `GetCompatibility` is called, THE response SHALL include the feature matrix digest.
5. WHEN `GetCompatibility` is called, THE response SHALL include the SDK matrix digest.
6. WHEN `GetCompatibility` is called, THE response SHALL include feature IDs and states.
7. WHEN `GetCompatibility` is called, THE response SHALL include SDK compatibility entries.
8. WHEN `GetCompatibility` is called, THE response SHALL include known divergences.
9. WHEN a namespace is supplied, THE response MAY include namespace-specific effective feature state.
10. WHEN a namespace is not supplied, THE response SHALL report global/default feature state.

### Requirement 30: Process coverage

**User Story:** As an operator, I want every relevant Tokeira process to expose comparable metadata, so that mixed-version deployments are diagnosable.

#### Acceptance Criteria

1. WHEN `tokeirad` exposes Tokeira-owned compatibility metadata, THE service SHALL use Buffa and connect-rust.
2. WHERE `tokeira-edge` or `tokeira-projection` are embedded in `tokeirad`, THE compatibility metadata SHALL be exposed through `tokeirad`'s compatibility service endpoint.
3. WHEN `tokeira-controller` exposes Tokeira-owned compatibility metadata, THE service SHALL use Buffa and connect-rust.
4. WHEN `tokeira-autoscaler` exposes Tokeira-owned compatibility metadata, THE service SHALL use Buffa and connect-rust.
5. WHERE a process does not expose a network service, THE same metadata SHALL remain available through logs and local CLI/version output.
6. THE compatibility service response SHALL include a `process_kind` or equivalent field.
7. THE compatibility service response SHALL include a process-specific endpoint identity when available.

### Requirement 31: Remote failure behaviour

**User Story:** As a CLI user, I want `tkr compat show --remote` to fail clearly against older deployments, so that I understand whether metadata is missing or unhealthy.

#### Acceptance Criteria

1. WHEN `tkr compat show --remote` calls a deployment that does not implement the Tokeira compatibility service, THE CLI SHALL report that remote compatibility metadata is unavailable.
2. WHEN standard Temporal `GetSystemInfo` succeeds but the Tokeira compatibility service fails, THE CLI SHALL print the standard Temporal server version and explain that Tokeira metadata could not be fetched.
3. WHEN both standard `GetSystemInfo` and the Tokeira compatibility service fail, THE CLI SHALL return a non-zero exit code.
4. WHEN remote metadata is unavailable, THE CLI SHALL NOT invent feature or SDK matrix data.
5. WHEN remote metadata is unavailable, THE CLI MAY suggest upgrading the deployment.

---

## Feature 7: Proto Version Sync Policy

### Requirement 32: Exact upstream Temporal mirror

**User Story:** As a maintainer, I want vendored Temporal protos to remain exact upstream mirrors, so that compatibility claims are trustworthy.

#### Acceptance Criteria

1. THE vendored Temporal proto directory SHALL contain only upstream Temporal proto files for the pinned `TEMPORAL_PROTO_VERSION`.
2. THE vendored Temporal proto directory SHALL NOT contain Tokeira-specific modifications to upstream files.
3. THE proto sync check SHALL compare vendored files against the upstream source for the pinned version.
4. THE proto sync check SHALL fail on any local patch to an upstream proto file.
5. Tokeira-owned proto files SHALL live outside the vendored upstream Temporal proto tree.
6. Tokeira-owned proto files SHALL use Tokeira-owned package names.
7. Tokeira-owned proto files SHALL be generated through the Buffa + connect-rust pipeline where they define RPC services or compatibility metadata.

### Requirement 33: Proto version pinning

**User Story:** As a developer, I want the vendored proto version to be explicit, so that generated code is reproducible.

#### Acceptance Criteria

1. THE repository SHALL declare `TEMPORAL_PROTO_VERSION` in exactly one checked-in source location.
2. THE generated Temporal proto code SHALL be reproducible from the vendored Temporal proto files.
3. THE generated Tokeira proto code SHALL be reproducible from Tokeira-owned proto files.
4. THE proto generation task SHALL fail if generated files are stale.
5. THE Dagger guardrail SHALL fail if `TEMPORAL_PROTO_VERSION` changes without required regenerated output.
6. THE `tkr compat show` output SHALL display `TEMPORAL_PROTO_VERSION`.

### Requirement 34: Proto bump workflow

**User Story:** As a maintainer, I want proto bumps to be reviewable, so that upstream API changes are classified intentionally.

#### Acceptance Criteria

1. WHEN `TEMPORAL_PROTO_VERSION` changes, THE pull request SHALL include a compatibility-surface review.
2. WHEN upstream adds an RPC, THE feature matrix SHALL classify it before CI passes.
3. WHEN upstream removes an RPC, THE feature matrix SHALL remove or retire the corresponding surface before CI passes.
4. WHEN upstream adds a request field, response field, enum variant, history event, command attribute, capability flag, or error detail, THE compatibility-surface review SHALL classify it.
5. WHEN upstream changes generated code, THE pull request SHALL include regenerated code.
6. THE proto bump workflow SHALL NOT automatically update `TEMPORAL_SERVER_COMPAT`.

### Requirement 35: Version monotonicity

**User Story:** As a maintainer, I want compatibility pins to avoid accidental downgrades, so that release history remains understandable.

#### Acceptance Criteria

1. WHEN `TEMPORAL_PROTO_VERSION` changes on a normal branch, THE new version SHALL be greater than or equal to the previous version.
2. WHEN `TEMPORAL_SERVER_COMPAT` changes on a normal branch, THE new version SHALL be greater than or equal to the previous version.
3. IF a downgrade is intentionally required, THEN the change SHALL include an explicit override file or commit trailer.
4. IF a downgrade override is present, THEN Dagger CI SHALL require a human-readable reason.
5. THE monotonicity check SHALL compare against the configured base branch.

### Requirement 36: Server compatibility bump protocol

**User Story:** As a compatibility owner, I want server compatibility claims to be bumped only with evidence, so that Tokeira does not mislead SDK users.

#### Acceptance Criteria

1. WHEN proposing a `TEMPORAL_SERVER_COMPAT` bump, THE pull request SHALL include the target Temporal server version.
2. WHEN proposing a `TEMPORAL_SERVER_COMPAT` bump, THE pull request SHALL include the current `TEMPORAL_PROTO_VERSION`.
3. WHEN proposing a `TEMPORAL_SERVER_COMPAT` bump, THE pull request SHALL include SDK matrix verification status.
4. WHEN proposing a `TEMPORAL_SERVER_COMPAT` bump, THE pull request SHALL include known divergences.
5. WHEN proposing a `TEMPORAL_SERVER_COMPAT` bump, THE pull request SHALL include conformance test evidence.
6. WHEN conformance evidence is incomplete, THE pull request SHALL explain why the compatibility claim remains safe.
7. THE bump protocol SHALL be manual in this specification.
8. THE bump protocol SHALL NOT require `tkr compat bump`.

---

## Feature 8: CLI Compatibility Commands

### Requirement 37: `tkr compat show`

**User Story:** As an operator, I want a single command to show compatibility metadata, so that I can inspect local and remote Tokeira versions.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL provide `tkr compat show`.
2. WHEN run without `--remote`, THE command SHALL print local build metadata.
3. WHEN run with `--remote`, THE command SHALL call the target deployment.
4. WHEN run with `--remote`, THE command SHALL call standard Temporal `GetSystemInfo` where available.
5. WHEN run with `--remote`, THE command SHALL call the Tokeira compatibility service where available.
6. WHEN calling the Tokeira compatibility service, THE command SHALL use the connect-rust client.
7. THE command SHALL support human-readable output.
8. THE command SHALL support JSON output.
9. THE command SHALL display `TOKEIRA_VERSION`.
10. THE command SHALL display `TOKEIRA_GIT_SHA`.
11. THE command SHALL display `TEMPORAL_PROTO_VERSION`.
12. THE command SHALL display `TEMPORAL_SERVER_COMPAT`.
13. THE command SHALL display `RUST_TOOLCHAIN`.
14. THE command SHALL display `SOURCE_TREE_HASH`.
15. THE command SHALL display feature matrix digest.
16. THE command SHALL display SDK matrix digest.
17. THE command SHALL display feature states when available.
18. THE command SHALL display SDK compatibility entries when available.
19. THE command SHALL display the build mode (`dev` or `versioned`) when that field is available.

### Requirement 38: `tkr compat diff`

**User Story:** As an operator, I want to compare compatibility metadata between two deployments or artefacts, so that rolling upgrades are safer.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL provide `tkr compat diff`.
2. THE command SHALL compare two local JSON compatibility documents.
3. THE command SHALL compare local metadata against remote metadata.
4. THE command MAY compare two remote deployments.
5. WHEN comparing remote deployments, THE command SHALL use the Tokeira compatibility service where available.
6. THE diff SHALL highlight changed Tokeira versions.
7. THE diff SHALL highlight changed Temporal proto versions.
8. THE diff SHALL highlight changed Temporal server compatibility claims.
9. THE diff SHALL highlight changed feature states.
10. THE diff SHALL highlight changed SDK matrix entries.
11. THE diff SHALL highlight changed source-tree hashes.
12. THE diff SHALL return a non-zero exit code when an incompatible difference is detected and `--fail-on-incompatible` is supplied.

### Requirement 39: No PR automation in MVP

**User Story:** As a maintainer, I want the CLI MVP to stay focused, so that compatibility reporting lands before release automation.

#### Acceptance Criteria

1. THE `tkr compat` MVP SHALL NOT include GitHub PR creation.
2. THE `tkr compat` MVP SHALL NOT include release-note scraping.
3. THE `tkr compat` MVP SHALL NOT include automatic branch creation.
4. THE `tkr compat` MVP SHALL NOT include automatic commit creation.
5. THE `tkr compat` MVP MAY include local validation commands that future automation can reuse.

---

## Feature 9: Dagger Build and CI Substrate

### Requirement 40: Dagger as authoritative build substrate

**User Story:** As a Tokeira maintainer, I want local and remote builds to execute through the same Dagger functions, so that developer machines, remote CI, and versioned builds do not drift.

#### Acceptance Criteria

1. THE Dagger build substrate SHALL support two build modes: `dev` and `versioned`.
2. THE `dev` build mode SHALL allow dirty repository state.
3. THE `dev` build mode SHALL derive build metadata from repository state where available.
4. THE `dev` build mode SHALL NOT require a clean git commit.
5. THE `versioned` build mode SHALL require a clean git commit.
6. THE `versioned` build mode SHALL reject dirty repository state.
7. THE `versioned` build mode SHALL derive build metadata from repository state and checked-in configuration.
8. THE `versioned` build mode SHALL verify embedded `BuildInfo` after build.
9. THE `versioned` build mode SHALL reject non-deterministic source-tree hash results.
10. THE repository SHALL define Dagger functions for compatibility checks.
11. THE repository SHALL define Dagger functions for generated-code freshness validation.
12. THE repository SHALL define Dagger functions for build metadata derivation.
13. THE local `tkr ci check` command SHALL invoke the same Dagger check function intended for use by future remote CI.
14. WHEN remote CI wiring exists, THE remote CI system SHALL invoke the same Dagger check function used by local development.
15. NEITHER build mode SHALL depend on CI-supplied metadata environment variables.

### Requirement 41: Dagger compatibility module

**User Story:** As a developer, I want compatibility checks to run through Dagger locally, so that local and remote CI use the same execution path.

#### Acceptance Criteria

1. THE repository SHALL include a Dagger module for compatibility checks.
2. THE Dagger module SHALL expose a `check` function.
3. THE `check` function SHALL run build metadata tests.
4. THE `check` function SHALL run source-tree hash tests.
5. THE `check` function SHALL run feature matrix tests.
6. THE `check` function SHALL run SDK matrix tests.
7. THE `check` function SHALL run proto sync checks.
8. THE `check` function SHALL run generated-code freshness checks.
9. THE `check` function SHALL run Buffa-generated code freshness checks.
10. THE `check` function SHALL run connect-rust-generated code freshness checks.
11. THE `check` function SHALL run standard `GetSystemInfo` handshake tests.
12. THE `check` function SHALL run Tokeira compatibility service tests.
13. THE `check` function SHALL return a machine-readable verdict.

### Requirement 42: Dagger versioned build

**User Story:** As a release engineer, I want versioned builds to execute through Dagger, so that artefacts, metadata, and provenance are produced through one controlled graph.

#### Acceptance Criteria

1. THE repository SHALL include a Dagger function for versioned builds.
2. WHEN the versioned build function runs, THE function SHALL derive build metadata from repository state and checked-in configuration.
3. WHEN the versioned build function runs, THE function SHALL generate the build metadata manifest.
4. WHEN the versioned build function runs, THE function SHALL invoke Cargo using the generated build metadata manifest.
5. WHEN the versioned build function runs, THE function SHALL run required compatibility checks before producing artefacts.
6. WHEN the versioned build function runs, THE function SHALL verify embedded `BuildInfo` after build.
7. WHEN the versioned build function runs, THE function SHALL reject dirty repository state.
8. WHEN the versioned build function runs, THE function SHALL reject missing git provenance.
9. WHEN the versioned build function runs, THE function SHALL reject non-deterministic source-tree hash results.
10. THE versioned build function SHALL NOT use ambient environment variables as metadata inputs.
11. THE versioned build function SHALL produce machine-readable artefact metadata.

### Requirement 43: Dagger lockfile policy

**User Story:** As a maintainer, I want Dagger CI inputs to be locked, so that compatibility checks are reproducible.

#### Acceptance Criteria

1. THE repository SHALL commit `.dagger/lock`.
2. THE repository SHALL commit Dagger module configuration.
3. WHEN compatibility checks run in hardened CI, THE Dagger invocation SHALL use frozen lock mode.
4. WHEN frozen lock mode is used, THE check SHALL fail if a Dagger dependency is not present in `.dagger/lock`.
5. WHEN updating locked Dagger dependencies intentionally, THE maintainer SHALL run an explicit lock update workflow.
6. WHEN a lock update changes `.dagger/lock`, THE pull request SHALL include the lockfile diff.
7. THE compatibility CI SHALL NOT silently refresh lockfile entries during normal check execution.
8. THE Dagger lockfile SHALL be treated as a reviewed supply-chain artefact.
9. THE versioned build path SHALL use frozen lock mode unless running an explicit lock update workflow.

### Requirement 44: Lock update workflow

**User Story:** As a maintainer, I want dependency lock updates to be explicit, so that mutable CI inputs do not change unnoticed.

#### Acceptance Criteria

1. THE CLI SHALL provide a documented way to refresh Dagger locks.
2. WHEN refreshing Dagger locks, THE workflow SHALL use Dagger’s lock update mechanism or an equivalent explicit live-resolution mode.
3. WHEN refreshing Dagger locks, THE workflow SHALL run compatibility checks after the lockfile changes.
4. WHEN refreshed locks change container images, THE pull request SHALL display the changed image references.
5. WHEN refreshed locks change Git references, THE pull request SHALL display the changed Git references.
6. WHEN refreshed locks change HTTP fetches, THE pull request SHALL display the changed fetch references.
7. THE normal pre-push check SHALL NOT update `.dagger/lock`.
8. THE versioned build path SHALL NOT update `.dagger/lock`.

### Requirement 45: Dagger lockfile limitations

**User Story:** As a CI owner, I want Dagger lockfiles to complement other reproducibility controls, so that mutable package indexes do not bypass the lock.

#### Acceptance Criteria

1. WHERE the Dagger module uses container base images, THE image references SHALL be pinned or resolved through `.dagger/lock`.
2. WHERE the Dagger module installs OS packages, THE module SHALL use a pinned CI image or a package snapshot strategy.
3. WHERE the Dagger module consumes Rust dependencies, THE check SHALL respect `Cargo.lock`.
4. WHERE the Dagger module consumes Buffa or connect-rust codegen tools, THE check SHALL respect their pinned versions.
5. WHERE the Dagger module consumes Node dependencies, THE check SHALL respect the relevant package-manager lockfile.
6. WHERE the Dagger module consumes Go dependencies, THE check SHALL respect `go.sum`.
7. THE compatibility checks SHALL NOT rely on floating package-manager state without an explicit exception.
8. EACH exception to dependency pinning SHALL include a reason and an owner.

### Requirement 46: `tkr ci check`

**User Story:** As a developer, I want one local command to run the compatibility guardrails, so that I can validate before pushing.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL provide `tkr ci check`.
2. WHEN `tkr ci check` runs, THE command SHALL invoke the Dagger compatibility `check` function.
3. BY DEFAULT, `tkr ci check` SHALL use frozen lock mode.
4. WHEN the user supplies an explicit lock-update option, THE command MAY run the lock update workflow.
5. WHEN Dagger is unavailable, THE command SHALL fail with a clear setup message.
6. WHEN checks fail, THE command SHALL return a non-zero exit code.
7. WHEN checks pass, THE command SHALL print a concise success summary.
8. THE command SHALL provide JSON output for future CI integration.
9. THE command SHALL NOT run an alternate non-Dagger compatibility check path.

### Requirement 47: `tkr ci build`

**User Story:** As a developer, I want one local command to run the same Dagger build path as remote CI, so that local build results match release and CI behaviour.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL provide `tkr ci build`.
2. WHEN `tkr ci build` runs without flags, THE command SHALL invoke the Dagger `dev` build function.
3. WHEN `tkr ci build --versioned` runs, THE command SHALL invoke the Dagger versioned build function.
4. WHEN `tkr ci build --versioned` runs, THE command SHALL derive build metadata inside Dagger.
5. WHEN `tkr ci build --versioned` runs, THE command SHALL validate embedded `BuildInfo`.
6. WHEN `tkr ci build --versioned` runs against a dirty repository, THE command SHALL fail.
7. WHEN Dagger is unavailable, THE command SHALL fail with a clear setup message.
8. THE command SHALL NOT use ambient environment variables as build metadata inputs.
9. THE command SHALL provide JSON output for future CI integration.

---

## Feature 10: Correctness Properties

### Requirement 48: Build metadata determinism

**User Story:** As a release engineer, I want build metadata to be deterministic, so that reproducibility regressions are caught.

#### Acceptance Criteria

1. WHEN build metadata is derived twice from the same repository state and checked-in configuration, THE outputs SHALL be byte-identical.
2. WHEN file ordering differs during source-tree hash computation, THE resulting hash SHALL remain identical.
3. WHEN excluded files change, THE source-tree hash SHALL remain unchanged.
4. WHEN included files change, THE source-tree hash SHALL change.
5. WHEN a wall-clock timestamp is introduced into build metadata, THE test suite SHALL fail.
6. WHEN ambient environment variables differ between two otherwise identical Dagger builds, THE derived build metadata SHALL remain unchanged.
7. WHEN the embedded `BuildInfo` differs from the generated metadata manifest, THE check SHALL fail.

### Requirement 49: Feature matrix digest stability

**User Story:** As a maintainer, I want feature matrix digests to be stable and reviewable, so that compatibility changes are visible.

#### Acceptance Criteria

1. WHEN the feature matrix is unchanged, THE digest SHALL be unchanged.
2. WHEN a feature state changes, THE digest SHALL change.
3. WHEN a feature ID changes, THE digest SHALL change.
4. WHEN compatibility-significant evidence changes, THE digest SHALL change.
5. WHEN entries are not sorted by feature ID, THE test suite SHALL fail.
6. THE digest computation SHALL NOT require compile-time sorting.

### Requirement 50: Compatibility surface completeness

**User Story:** As a compatibility reviewer, I want important Temporal surfaces classified, so that compatibility work is not reduced to endpoint counting.

#### Acceptance Criteria

1. WHEN upstream RPCs are enumerated, THE RPC completeness property SHALL classify every RPC.
2. WHEN upstream capability fields are enumerated, THE capability consistency property SHALL classify every capability field.
3. WHEN a proto bump introduces new history event types, THE compatibility review process SHALL classify them.
4. WHEN a proto bump introduces new command attributes, THE compatibility review process SHALL classify them.
5. WHEN a proto bump introduces new enum variants that affect SDK behaviour, THE compatibility review process SHALL classify them.
6. WHEN a proto bump introduces new error details that affect retryability or SDK handling, THE compatibility review process SHALL classify them.
7. THE initial implementation MAY automate RPC and capability enumeration first.
8. THE initial implementation SHALL document remaining surface kinds as manual review items until automation exists.

### Requirement 51: Standard handshake wire-shape property

**User Story:** As a maintainer, I want tests proving that Tokeira has not forked Temporal’s handshake, so that SDK compatibility remains clean.

#### Acceptance Criteria

1. WHEN tests run, THE descriptor for vendored `GetSystemInfoRequest` SHALL match upstream for the pinned proto version.
2. WHEN tests run, THE descriptor for vendored `GetSystemInfoResponse` SHALL match upstream for the pinned proto version.
3. WHEN tests run, THE descriptor for vendored `Capabilities` SHALL match upstream for the pinned proto version.
4. IF any Tokeira-specific field appears in an upstream Temporal message, THEN the test suite SHALL fail.
5. THE test failure SHALL name the message and field that diverged.

### Requirement 52: Buffa/connect-rust service compatibility property

**User Story:** As a maintainer, I want tests proving that Tokeira-owned compatibility RPCs are generated and served through the adopted stack, so that the control-plane RPC strategy remains consistent.

#### Acceptance Criteria

1. WHEN tests run, THE Tokeira compatibility proto generation SHALL use Buffa.
2. WHEN tests run, THE Tokeira compatibility service generation SHALL use connect-rust.
3. WHEN tests run, THE generated Tokeira compatibility message types SHALL be Buffa-generated types.
4. WHEN tests run, THE generated Tokeira compatibility service traits and clients SHALL be connect-rust generated.
5. IF Tokeira compatibility message types import from `prost`-generated modules, THEN the Dagger freshness check SHALL fail.
6. IF Tokeira compatibility service code imports from `tonic`-generated modules, THEN the Dagger freshness check SHALL fail.
7. IF generated code is stale, THEN the Dagger freshness check SHALL fail.

### Requirement 53: SDK matrix round-trip

**User Story:** As a CLI developer, I want SDK compatibility data to round-trip through JSON and protobuf, so that remote and local tooling share one format.

#### Acceptance Criteria

1. WHEN the SDK matrix is serialized to JSON, THE output SHALL deserialize into the owned SDK matrix type.
2. WHEN the owned SDK matrix is serialized again, THE semantic content SHALL be unchanged.
3. WHEN the digest is recomputed from the owned type, THE digest SHALL match the static SDK matrix digest.
4. WHEN the SDK matrix is encoded through Buffa-generated compatibility messages, THE decoded semantic content SHALL match the source matrix.
5. WHEN an SDK entry omits required evidence, THE test suite SHALL fail.
6. WHEN a known incompatible SDK version omits a reason, THE test suite SHALL fail.

### Requirement 54: Dagger frozen-lock check

**User Story:** As a CI owner, I want CI to fail when Dagger dependencies are not locked, so that mutable dependencies cannot enter silently.

#### Acceptance Criteria

1. WHEN the Dagger compatibility check runs in hardened mode, THE invocation SHALL use frozen lock mode.
2. WHEN a Dagger lookup is missing from `.dagger/lock`, THE check SHALL fail.
3. WHEN `.dagger/lock` is modified during a normal check, THE check SHALL fail.
4. WHEN `.dagger/lock` is modified during an explicit lock update, THE workflow SHALL require review of the diff.
5. THE CI summary SHALL report whether frozen lock mode was used.

---

## Feature 11: Documentation and Operator Guidance

### Requirement 55: Compatibility contract documentation

**User Story:** As a user, I want the compatibility promise explained plainly, so that I know what Tokeira does and does not claim.

#### Acceptance Criteria

1. THE repository SHALL document `TEMPORAL_PROTO_VERSION`.
2. THE repository SHALL document `TEMPORAL_SERVER_COMPAT`.
3. THE documentation SHALL explain that proto compatibility and server behavioural compatibility are separate.
4. THE documentation SHALL explain feature states.
5. THE documentation SHALL explain SDK verification states.
6. THE documentation SHALL explain that Tokeira-specific metadata is not exposed through patched Temporal protos.
7. THE documentation SHALL explain that Tokeira-specific metadata is exposed through Buffa + connect-rust services.
8. THE documentation SHALL explain that build metadata is derived through Dagger.
9. THE documentation SHALL explain that build metadata is not supplied through environment variables.
10. THE documentation SHALL include examples of `tkr compat show`.
11. THE documentation SHALL include examples of `tkr compat diff`.

### Requirement 56: Buffa/connect-rust guidance

**User Story:** As a contributor, I want clear guidance on the Tokeira-owned RPC stack, so that new metadata services do not fragment the architecture.

#### Acceptance Criteria

1. THE repository SHALL document that Tokeira-owned build metadata RPCs use Buffa and connect-rust.
2. THE repository SHALL document that Tokeira-owned capability metadata RPCs use Buffa and connect-rust.
3. THE repository SHALL document that upstream Temporal protos remain separate.
4. THE repository SHALL document when prost or tonic may still appear in upstream Temporal compatibility code.
5. THE repository SHALL document how to regenerate Buffa and connect-rust code.
6. THE repository SHALL document how generated-code freshness is checked.
7. THE repository SHALL document how codegen tool versions are pinned.

### Requirement 57: Dagger build and CI guidance

**User Story:** As a contributor, I want clear Dagger build and CI instructions, so that local and remote execution paths remain aligned.

#### Acceptance Criteria

1. THE repository SHALL document `tkr ci check`.
2. THE repository SHALL document `tkr ci build`.
3. THE repository SHALL document the Dagger versioned build path.
4. THE repository SHALL document the Dagger build metadata derivation path.
5. THE repository SHALL document the generated build metadata manifest.
6. THE repository SHALL document the Dagger lockfile policy.
7. THE repository SHALL document how to refresh `.dagger/lock`.
8. THE repository SHALL document when lockfile updates are appropriate.
9. THE repository SHALL document why frozen lock mode is used for hardened checks.
10. THE repository SHALL document the limitations of Dagger lockfiles.
11. THE repository SHALL document package-manager lockfile expectations.
12. THE repository SHALL document that ambient environment variables are not metadata authority.

### Requirement 58: Compatibility bump checklist

**User Story:** As a reviewer, I want a checklist for compatibility bumps, so that review quality is consistent.

#### Acceptance Criteria

1. THE repository SHALL include a `TEMPORAL_PROTO_VERSION` bump checklist.
2. THE repository SHALL include a `TEMPORAL_SERVER_COMPAT` bump checklist.
3. THE proto bump checklist SHALL require upstream version, generated-code status, and surface classification.
4. THE server compatibility bump checklist SHALL require conformance evidence and SDK matrix impact.
5. THE checklist SHALL require known divergences to be documented.
6. THE checklist SHALL require feature matrix changes when upstream surfaces are added.
7. THE checklist SHALL state that calendar drift alone is not sufficient for a server compatibility bump.
8. THE checklist SHALL include Buffa/connect-rust generated-code impact where Tokeira-owned compatibility protos change.
9. THE checklist SHALL require Dagger compatibility checks to pass.
10. THE checklist SHALL require build metadata validation to pass.

---

## Implementation Notes

These notes are non-normative.

1. Keep `tokeira-build-info` dependency-free.
2. Put JSON rendering in CLI or process crates.
3. Put protobuf rendering in the Tokeira compatibility service crate.
4. Generate Tokeira-owned compatibility messages with Buffa.
5. Generate Tokeira-owned compatibility service clients and handlers with connect-rust.
6. Keep standard Temporal SDK-facing protos boring and upstream-exact.
7. Do not patch `GetSystemInfoResponse`.
8. Do not use `GetSystemInfo` as a Tokeira metadata dumping ground.
9. Use the Tokeira compatibility service for build provenance, feature state, SDK matrix, and capability evidence.
10. Avoid compile-time sorting for matrix digests; require sorted input and test it.
11. Treat `TEMPORAL_SERVER_COMPAT` as an evidence-backed claim.
12. Use Dagger as the authoritative local and remote build/CI execution substrate.
13. Derive build metadata inside the Dagger graph from repository state and checked-in configuration.
14. Use a deterministic generated build metadata manifest for Cargo embedding.
15. Do not use ambient environment variables as build metadata authority.
16. Use Dagger lockfiles, but do not rely on them as the only supply-chain control.
17. Defer `tkr compat bump` automation until metadata, generated-code checks, and conformance evidence are stable.

---

## Deferred Work

The following work is intentionally deferred:

1. `tkr compat bump`,
2. GitHub API integration,
3. automatic PR creation,
4. automated release-note classification,
5. automatic compatibility-surface derivation for every protobuf field kind,
6. full SDK conformance orchestration,
7. remote CI provider wiring,
8. compatibility dashboards,
9. automatic mixed-version fleet analysis,
10. controller/autoscaler capability dashboards.

Each deferred item may consume the metadata, Dagger build functions, generated code, and RPC surfaces defined by this specification, but none is required for the MVP.
