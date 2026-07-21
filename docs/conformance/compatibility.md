# Temporal Compatibility

This document explains how Tokeira records, exposes, and verifies its Temporal
compatibility surface.

## Version Pins

Tokeira carries two independent compatibility pins:

- `TEMPORAL_PROTO_VERSION` is the vendored upstream Temporal proto version.
- `TEMPORAL_SERVER_COMPAT` is the conservative SDK-visible server behavior
  claim.

The proto pin may advance before the server compatibility claim. A server
compatibility bump requires matrix updates and evidence that the SDK-visible
behavior matches the claimed Temporal server release.

## Feature Matrix

`crates/tokeira-compatibility/src/matrix.rs` owns `FEATURE_MATRIX`.
Every WorkflowService and OperatorService RPC from the vendored proto set must be
classified exactly once. Current states are:

- `Implemented`: proven compatible enough for the current claim.
- `Partial`: implemented or partially implemented, but not yet strict
  conformance-complete.
- `Experimental`: intentionally present but not part of the stable claim.
- `Stubbed`: accepted as a known surface but not implemented.
- `Unsupported`: outside the current compatibility target.

`tkr compat show` reads the local binary metadata and prints the matrix summary.
`tkr compat show --json` emits a document suitable for `tkr compat diff`.

## SDK Matrix

`crates/tokeira-compatibility/src/sdk.rs` owns `SDK_MATRIX`. Each SDK entry
records the language, minimum supported version, maximum tested version, known
incompatible versions, verification state, and evidence references.

Unknown maximum-tested versions are represented as `unknown`, not as a guessed
version.

## Tokeira Compatibility Service

Tokeira-owned compatibility metadata is exposed through Buffa/connect-rust:

- Protos live under `proto/tokeira/compatibility/v1/`.
- Generated bindings live in `crates/tokeira-compatibility-proto/`.
- Service mapping logic lives in `crates/tokeira-compatibility-service/`.

The upstream Temporal API remains in `tokeira-proto`; Tokeira does not add
private fields to upstream `GetSystemInfoResponse`. Standard SDK handshakes stay
upstream-only, while richer Tokeira metadata uses the Tokeira-owned service.

Generated code freshness is intended to be checked by the Dagger compatibility
pipeline. Until that module lands, normal Rust tests validate the service adapter
and matrix serialization paths.

## Dagger CI and Build Metadata

The planned Dagger compatibility module is responsible for:

- Checking matrix completeness and SDK matrix invariants.
- Checking generated-code freshness.
- Checking proto/server compatibility pin monotonicity.
- Producing deterministic versioned-build manifests.
- Computing the source-tree hash.
- Enforcing frozen lock mode for hardened checks.

`tkr ci check`, `tkr ci build`, and `tkr ci lock-update` are scaffolded now. They
parse arguments and fail with a clear setup message when the Dagger module is not
available. This keeps the CLI contract stable while the Dagger module is built
out.

Versioned builds pass `TOKEIRA_BUILD_MANIFEST_PATH` to Cargo. The manifest
contains every field embedded by `tokeira-build-info`. Local dev builds omit the
manifest and use workspace-derived fallback metadata.

## Compatibility Bump Checklists

### `TEMPORAL_PROTO_VERSION`

Before bumping the proto version:

1. Update the vendored proto tree.
2. Regenerate upstream Temporal Rust bindings.
3. Regenerate Tokeira-owned Buffa/connect-rust bindings when affected.
4. Run matrix completeness checks and classify any new RPCs, fields, enums, or
   messages.
5. Update docs and evidence references for any changed compatibility surfaces.
6. Run the full workspace verification suite.

### `TEMPORAL_SERVER_COMPAT`

Before bumping the server compatibility claim:

1. Confirm the feature matrix supports the target Temporal server release.
2. Confirm SDK matrix evidence covers the claimed behavior.
3. Document known divergences and mitigations.
4. Ensure `GetSystemInfo` capabilities remain conservative and SDK-safe.
5. Run compatibility checks and the full workspace verification suite.
6. Include the rationale and evidence references in the bump change.
