# tokeira-build-info

`tokeira-build-info` embeds immutable build and compatibility metadata in every
Tokeira binary. The crate owns only the metadata values and the `BuildInfo`
struct. It deliberately does not render JSON, tables, terminal output, protobuf
messages, or service responses; those formats belong to the CLI and service
adapter crates that consume `BuildInfo`.

## Metadata Sources

Versioned builds set `TOKEIRA_BUILD_MANIFEST_PATH` before compiling. The manifest
is a checked, deterministic key/value file produced by the Dagger versioned build
pipeline. It must contain:

```text
TOKEIRA_VERSION=...
TOKEIRA_GIT_SHA=...
TEMPORAL_PROTO_VERSION=...
TEMPORAL_SERVER_COMPAT=...
RUST_TOOLCHAIN=...
SOURCE_TREE_HASH=...
FEATURE_MATRIX_DIGEST=...
SDK_MATRIX_DIGEST=...
BUILD_MODE=versioned
```

Local development builds do not need that manifest. When
`TOKEIRA_BUILD_MANIFEST_PATH` is absent, `build.rs` derives safe fallback metadata
from the workspace: Cargo package version, the current Git SHA when available,
`src/pinned.rs`, `rust-toolchain.toml`, placeholder digest values, and
`BUILD_MODE=dev`.

## Pinned Compatibility Versions

`src/pinned.rs` is the canonical location for:

- `TEMPORAL_PROTO_VERSION`: the vendored upstream Temporal proto version.
- `TEMPORAL_SERVER_COMPAT`: the conservative SDK-visible Temporal server
  compatibility claim.

These values are intentionally independent. Updating generated protos does not
automatically justify increasing the server compatibility claim; the feature
matrix and conformance evidence must support that claim first.

## Determinism Rules

The crate and its build script must not use wall-clock time. Build timestamps are
not part of the metadata model because they make byte-for-byte reproducibility
harder and do not improve operator diagnosis.

Versioned builds should use the Dagger pipeline once it is available. Dev builds
remain ergonomic and deterministic enough for local work, but their placeholder
hashes are not release provenance.
